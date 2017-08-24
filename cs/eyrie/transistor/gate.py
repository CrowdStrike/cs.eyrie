from collections import deque, namedtuple
from datadog import statsd
from datetime import datetime
from cs.eyrie.interfaces import IGate, ITransistor
from cs.eyrie.transistor import (
    CLOSED, CLOSING, DEFAULT_TRANSDUCER_CONCURRENCY, RUNNING,
)
from tornado import gen
from tornado.concurrent import is_future
from tornado.ioloop import PeriodicCallback
from tornado.queues import QueueEmpty, QueueFull
from zope.interface import implementer


ThroughputSample = namedtuple(
    'ThroughputSample', [
        'timestamp',
        'num_emitted',
    ],
)


@implementer(ITransistor)
class Transistor(object):
    """Implementation of ITransistor.
    This class is responsible for shutting down in the face
    of errors, and graceful termination on end of input.
    """

    def __init__(self, logger, loop, gate, source, drain,
                 metric_prefix='transistor'):
        self.logger = logger
        self.loop = loop
        self.gate = gate
        self.source = source
        self.drain = drain
        self.metric_prefix = metric_prefix
        self.state = RUNNING

    @gen.coroutine
    def close(self, msg_prefix):
        try:
            self.state = CLOSING
            self.logger.error('%s; closing source', msg_prefix)
            yield self.source.close()
            self.logger.error('%s; closing drain', msg_prefix)
            yield self.drain.close()
        except Exception as err:
            self.logger.exception(err)
        finally:
            self.state = CLOSED

    @gen.coroutine
    def join(self, timeout=None):
        futures = dict(
            eof=self.source.end_of_input.wait(timeout),
            input_error=self.source.input_error.wait(timeout),
            output_error=self.drain.output_error.wait(timeout),
        )
        wait_iterator = gen.WaitIterator(**futures)
        while not wait_iterator.done():
            try:
                yield wait_iterator.next()
            except gen.TimeoutError:
                self.logger.warning('Wait timeout occurred; aborting')
                break
            except Exception as err:
                self.logger.error("Error %s from %s",
                                  err, wait_iterator.current_future)
            else:
                if wait_iterator.current_index == 'queue' and \
                  (self.source.state == CLOSING or
                   self.drain.state == CLOSING):
                    self.logger.info('Queue drained')
                    break
                if wait_iterator.current_index == 'eof':
                    self.logger.info('End of input reached')
                    break
                elif wait_iterator.current_index == 'input_error' and \
                   self.source.state != CLOSING:
                    yield self.close('Error occurred in source')
                    break
                elif wait_iterator.current_index == 'output_error' and \
                   self.drain.state != CLOSING:
                    yield self.close('Error occurred in drain')
                    break


class ThroughputTracker(object):

    def __init__(self, logger, loop, num_samples=3):
        self.logger = logger
        self.loop = loop
        # callback_time is in milliseconds
        self.throughput_pc = PeriodicCallback(self.onThroughput,
                                              30 * 1000,
                                              self.loop)
        self.throughput_pc.start()
        self.samples = deque(maxlen=num_samples)
        self.samples.appendleft(ThroughputSample(timestamp=datetime.utcnow(),
                                                 num_emitted=0))
        self.num_emitted = 0

    def onThroughput(self):
        # Throughput measurements
        now = datetime.utcnow()
        current = ThroughputSample(timestamp=now, num_emitted=self.num_emitted)
        deltas = [
            current.timestamp - sample.timestamp
            for sample in self.samples
        ]
        samples = [
            '%s|%0.1f' % (
                deltas[i],
                ((current.num_emitted-sample.num_emitted) /
                 deltas[i].total_seconds()),
            )
            for i, sample in enumerate(self.samples)
        ]
        self.samples.appendleft(current)
        self.logger.info('Throughput samples: %s', ', '.join(samples))


@implementer(IGate)
class BufferedGate(object):
    """Implementation of IGate that uses a tornado.queues.Queue to buffer
    incoming messages. Also reports throughput metrics. Use this gate
    if your transducer is a coroutine.
    """

    def __init__(self, logger, loop, drain, queue, transducer, **kwargs):
        self.logger = logger
        self.loop = loop
        self.drain = drain
        self.metric_prefix = kwargs.get('metric_prefix', 'gate')
        self.transducer = transducer
        self.state = RUNNING
        self._delay = kwargs.pop('delay', 0)
        self._queue = queue
        self._throughput_tracker = ThroughputTracker(logger, loop, **kwargs)
        self.transducer_concurrency = kwargs.get('transducer_concurrency',
                                                 self._queue.maxsize or \
                                                  DEFAULT_TRANSDUCER_CONCURRENCY)
        for i in range(self.transducer_concurrency):
            self.loop.spawn_callback(self._poll)

    @gen.coroutine
    def _maybe_send(self, outgoing_msg):
        if outgoing_msg is None:
            self.logger.debug('No outgoing message; dropping')
        elif isinstance(outgoing_msg, list):
            yield self._send(*outgoing_msg)
        else:
            yield self._send(outgoing_msg)

    @gen.coroutine
    def _poll(self):
        """Infinite coroutine for draining the queue.
        """
        while True:
            try:
                incoming_msg = self._queue.get_nowait()
            except QueueEmpty:
                self.logger.debug('Source queue empty, waiting...')
                incoming_msg = yield self._queue.get()

            outgoing_msg_future = self.transducer(incoming_msg)
            if is_future(outgoing_msg_future):
                outgoing_msg = yield outgoing_msg_future
            yield self._maybe_send(outgoing_msg)

    @gen.coroutine
    def _send(self, *messages):
        for msg in messages:
            try:
                self.drain.emit_nowait(msg)
            except QueueFull:
                self.logger.debug('Drain full, waiting...')
                yield self.drain.emit(msg)
            else:
                self._throughput_tracker.num_emitted += 1
                statsd.increment('%s.queued' % self.metric_prefix)

    def put_nowait(self, msg):
        self._queue.put_nowait(msg)

    @gen.coroutine
    def put(self, msg, timeout=None):
        yield self._queue.put(msg, timeout)


@implementer(IGate)
class Gate(object):
    """Implementation of IGate that accepts a message and blocks
    until the configured drain accepts it. Also reports throughput metrics.
    """

    # We intentionally do not support higher concurrency
    transducer_concurrency = 1

    def __init__(self, logger, loop, drain, transducer, **kwargs):
        self.logger = logger
        self.loop = loop
        self.drain = drain
        self.metric_prefix = kwargs.get('metric_prefix', 'gate')
        self.transducer = transducer
        self.state = RUNNING
        self._throughput_tracker = ThroughputTracker(logger, loop, **kwargs)

    def _increment(self):
        self._throughput_tracker.num_emitted += 1
        statsd.increment('%s.queued' % self.metric_prefix)

    def put_nowait(self, msg):
        outgoing_msg = self.transducer(msg)
        self._increment()
        if outgoing_msg is None:
            self.logger.debug('No outgoing message; dropping')
        else:
            self.drain.emit_nowait(outgoing_msg)

    @gen.coroutine
    def put(self, msg, retry_timeout=None):
        outgoing_msg = self.transducer(msg)
        self._increment()
        if outgoing_msg is None:
            self.logger.debug('No outgoing message; dropping')
        else:
            yield self.drain.emit(outgoing_msg, retry_timeout)
