# -*- coding: utf-8 -*-
"""
There are 2 primary purposes of this module:
    1. Provide back-pressure so that memory is conserved when downstream
       services slow down
    2. Provide a unified interface for swapping in source/draining services

It is possible to pair a Kafka consumer with a ZMQ sender, or vice-versa, pair
a ZMQ receiver with a Kafka producer. All communication is async, using Tornado
queues throughout.
"""
from Queue import Full
from collections import deque, namedtuple
from datetime import datetime
from os import linesep

from cs.eyrie.config import INITIAL_TIMEOUT, MAX_TIMEOUT
from cs.eyrie.interfaces import IDrain, IGate, IKafka, ISource, ITransistor
from datadog import statsd
from tornado import gen
from tornado.ioloop import PeriodicCallback
from tornado.locks import Event
from tornado.queues import Queue
from zope.interface import implementer


RUNNING, CLOSING, CLOSED = range(3)


KafkaMessage = namedtuple(
    'KafkaMessage', [
        'key',
        'offset',
        'partition',
        'topic',
        'value',
    ],
)


ThroughputSample = namedtuple(
    'ThroughputSample', [
        'timestamp',
        'num_emitted',
    ],
)


def get_last_element(msg):
    if isinstance(msg, (list, tuple)):
        return msg[-1]
    return msg


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


@implementer(IGate)
class Gate(object):
    """Implementation of IGate that accepts a message and blocks
    until the configured drain accepts it. Also reports throughput metrics.
    """

    def __init__(self, logger, loop, drain, transducer,
                 metric_prefix='transistor', num_samples=3):
        self.logger = logger
        self.loop = loop
        self.drain = drain
        self.metric_prefix = metric_prefix
        self.transducer = transducer
        self.state = RUNNING
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
                (current.num_emitted-sample.num_emitted)/deltas[i].total_seconds()
            )
            for i, sample in enumerate(self.samples)
        ]
        self.samples.appendleft(current)
        self.logger.info('Throughput samples: %s', ', '.join(samples))

    def put_nowait(self, msg):
        outgoing_msg = self.transducer(msg)
        self.drain.emit_nowait(outgoing_msg)
        self.num_emitted += 1

    @gen.coroutine
    def put(self, msg, retry_timeout=INITIAL_TIMEOUT):
        outgoing_msg = self.transducer(msg)
        yield self.drain.emit(outgoing_msg, retry_timeout)
        self.num_emitted += 1


# Drain implementations
@implementer(IDrain, IKafka)
class RDKafkaDrain(object):
    """Implementation of IDrain that produces to a Kafka topic using librdkafka
    asynchronously. Backpressure is implemented with a tornado.queues.Queue.
    Expects an instance of confluent_kafka.Producer as self.sender.
    """

    def __init__(self, logger, loop, producer, topic,
                 metric_prefix='emitter'):
        self.emitter = producer
        self.logger = logger
        self.loop = loop
        self.loop.spawn_callback(self._poll)
        self._completed = Queue()
        self.metric_prefix = metric_prefix
        self.output_error = Event()
        self.sender_tag = 'sender:%s.%s' % (self.__class__.__module__,
                                            self.__class__.__name__)
        self.topic = topic
        self.state = RUNNING

    @gen.coroutine
    def close(self, retry_timeout=INITIAL_TIMEOUT):
        try:
            self.state = CLOSING
            begin = datetime.utcnow()
            num_messages = len(self.emitter)
            elapsed = datetime.utcnow() - begin
            while num_messages > 0 and elapsed <= MAX_TIMEOUT:
                self.logger.info("Flushing send queue in %s/%s: %d",
                                 elapsed, MAX_TIMEOUT, num_messages)
                self.emitter.poll(0)
                num_messages = len(self.emitter)
                elapsed = datetime.utcnow() - begin
                retry_timeout = min(retry_timeout*2, MAX_TIMEOUT)
                yield gen.sleep(retry_timeout.total_seconds())
            else:
                self.logger.error('Unable to flush messages; aborting')
        finally:
            self.state = CLOSED

    def emit_nowait(self, msg):
        self.logger.debug("Drain emitting")
        try:
            self.emitter.produce(
                self.topic, msg,
                # This callback is executed in the librdkafka thread
                callback=lambda err, kafka_msg: self._trampoline(err,
                                                                 kafka_msg),
            )
        except BufferError:
            raise Full

    @gen.coroutine
    def emit(self, msg, retry_timeout=INITIAL_TIMEOUT):
        while True:
            try:
                self.emit_nowait(msg)
            except Full:
                yield gen.sleep(retry_timeout)
                retry_timeout = min(retry_timeout*2, MAX_TIMEOUT)

    @gen.coroutine
    def _poll(self, retry_timeout=INITIAL_TIMEOUT):
        """Infinite coroutine for draining the delivery report queue,
        with exponential backoff.
        """
        try:
            num_processed = self.emitter.poll(0)
            if num_processed > 0:
                self.logger.debug("Drain received ack for messages: %d",
                                  num_processed)
                retry_timeout = INITIAL_TIMEOUT
            else:
                self.logger.debug("Drain delivery report queue empty")
                # Retry with exponential backoff
                yield gen.sleep(retry_timeout.total_seconds())
                retry_timeout = min(retry_timeout*2, MAX_TIMEOUT)
        finally:
            self.loop.spawn_callback(self._poll, retry_timeout)

    @gen.coroutine
    def _on_track(self, err, kafka_msg):
        self.logger.debug('Received delivery notification: "%s", "%s"',
                          err, kafka_msg)
        if err:
            self.logger.error('Error encountered, giving up: %s', err)
            raise err

    def _trampoline(self, err, kafka_msg):
        # This is necessary, so that we trampoline from the librdkafka thread
        # back to the main Tornado thread:
        # add_callback() may be used to transfer control from other threads to
        # the IOLoop's thread.
        # It is safe to call this method from any thread at any time, except
        # from a signal handler. Note that this is the only method in IOLoop
        # that makes this thread-safety guarantee; all other interaction with
        # the IOLoop must be done from that IOLoop's thread.
        self.loop.add_callback(
            self._on_track, err, kafka_msg
        )


@implementer(IDrain)
class StreamDrain(object):
    """Implementation of IDrain that writes to stdout.
    """

    def __init__(self, logger, loop, stream,
                 metric_prefix='emitter'):
        self.emitter = stream
        self.logger = logger
        self.loop = loop
        self.metric_prefix = metric_prefix
        self.output_error = Event()
        self.state = RUNNING
        self.sender_tag = 'sender:%s.%s' % (self.__class__.__module__,
                                            self.__class__.__name__)

    @gen.coroutine
    def close(self):
        self.state = CLOSING
        self.logger.debug("Flushing send queue")
        self.emitter.flush()

    def emit_nowait(self, msg):
        self.logger.debug("Drain emitting")
        self.emitter.write(msg)
        if not msg.endswith(linesep):
            self.emitter.write(linesep)

    @gen.coroutine
    def emit(self, msg, timeout=None):
        self.emit_nowait(msg)


@implementer(IDrain)
class ZMQDrain(object):
    """Implementation of IDrain that pushes to an asynchronous
    zmq.eventloop.zmqstream.ZMQStream. This implementation overrides the
    high-water mark behavior from cs.eyrie.vassal.Vassal to use a
    tornado.queues.Queue.
    """

    def __init__(self, logger, loop, inflight, zmq_stream,
                 metric_prefix='emitter'):
        self.emitter = zmq_stream
        self.logger = logger
        self.loop = loop
        self.inflight = inflight
        self.metric_prefix = metric_prefix
        self.output_error = Event()
        self.state = RUNNING
        self.sender_tag = 'sender:%s.%s' % (self.__class__.__module__,
                                            self.__class__.__name__)

    @gen.coroutine
    def close(self):
        self.state = CLOSING
        self.logger.debug("Flushing send queue")
        self.emitter.flush()

    @gen.coroutine
    def emit(self, msg, timeout=None):
        yield self.inflight.acquire(timeout)
        self.logger.debug("Drain emitting")
        if isinstance(msg, basestring):
            msg = [msg]
        # NOTE: this overwrites any current callback attached to the stream
        self.emitter.send_multipart(msg, callback=self.onTrack,
                                    copy=False, track=True)

    @gen.coroutine
    def onTrack(self, frames, msg_tracker, retry_timeout=INITIAL_TIMEOUT):
        """Callback interface for when messages are delivered.
        """
        # If the handler triggers an exception, pyzmq will disable it
        # Here we catch any exception and just log it, so that processing
        # can continue
        try:
            # This should always be an instance of zmq.MessageTracker
            # or our queue will starve
            if msg_tracker is None:
                log_msg = 'No MessageTracker received for a sent message'
                self.logger.warning(log_msg)
            else:
                if msg_tracker.done:
                    self.logger.debug("Sender received message ack")
                    self.inflight.release()
                else:
                    self.logger.debug("Sender MessageTracker incomplete")
                    # Retry with exponential backoff
                    self.loop.call_later(retry_timeout.total_seconds(),
                                         self.onTrack,
                                         frames,
                                         msg_tracker,
                                         retry_timeout=min(retry_timeout*2,
                                                           MAX_TIMEOUT))
        except Exception as err:
            statsd.increment('rf.exception_count', tags=[
                self.sender_tag,
                'exception:%s.%s' % (err.__class__.__module__,
                                     err.__class__.__name__),
            ])
            self.logger.exception(err)


# Source implementations
@implementer(ISource)
class PailfileSource(object):
    """Implementation of ISource that reads data from a Hadoop pailfile.
    """

    def __init__(self, logger, loop, queue, sequence_reader,
                 metric_prefix='source', infinite=False):
        self.gate = queue
        self.collector = sequence_reader
        self.logger = logger
        self.loop = loop
        self.metric_prefix = metric_prefix
        self.end_of_input = Event()
        self.input_error = Event()
        self.state = RUNNING
        self.sender_tag = 'sender:%s.%s' % (self.__class__.__module__,
                                            self.__class__.__name__)
        self._infinite = infinite
        self.loop.spawn_callback(self.onInput)

    @gen.coroutine
    def close(self):
        self.state = CLOSING
        self.logger.warning('Closing source')
        if not self.collector.closed():
            self.collector.close()

    @gen.coroutine
    def onInput(self):
        respawn = True
        try:
            key = self.collector.getKeyClass()()
            result = self.collector.nextKey(key)
            if result:
                yield self.gate.put(key.toString())
                self.logger.info('PailfileSource queued message: %d',
                                 self.gate.qsize())
                statsd.increment('%s.queued' % self.metric_prefix,
                                 tags=[self.sender_tag])
            else:
                if self._infinite:
                    self.collector.sync(0)
                else:
                    self.end_of_input.set()
                    respawn = False
        except Exception as err:
            self.logger.exception(err)
            self.input_error.set()
            respawn = False
        finally:
            if respawn:
                self.loop.spawn_callback(self.onInput)


@implementer(ISource, IKafka)
class RDKafkaSource(object):
    """Implementation of ISource that consumes messages from a Kafka topic.
    """

    def __init__(self, logger, loop, queue, consumer,
                 *topics, **kwargs):
        self.gate = queue
        self.collector = consumer
        self.logger = logger
        self.loop = loop
        self.metric_prefix = kwargs.get('metric_prefix', 'source')
        self.end_of_input = Event()
        self.input_error = Event()
        self.state = RUNNING
        self.sender_tag = 'sender:%s.%s' % (self.__class__.__module__,
                                            self.__class__.__name__)
        self.loop.add_callback(self.onInput)
        self.collector.subscribe(list(topics))

    def _trampoline(self, frames):
        """Bounce incoming messages to be handled properly by the IOLoop.
        """
        self.loop.add_callback(self.onInput, frames)

    @gen.coroutine
    def close(self):
        self.state = CLOSING
        self.logger.warning('Closing source')
        self.collector.close()

    @gen.coroutine
    def onInput(self, retry_timeout=INITIAL_TIMEOUT):
        """Infinite coroutine for draining the delivery report queue,
        with exponential backoff.
        """
        respawn = True
        try:
            msg = self.collector.poll(0)
            if msg is not None:
                err = msg.error()
                if not err:
                    retry_timeout = INITIAL_TIMEOUT
                    yield self.gate.put(
                        KafkaMessage(
                            msg.key(),
                            msg.offset(),
                            msg.partition(),
                            msg.topic(),
                            msg.value(),
                        )
                    )
                else:
                    retry_timeout = min(retry_timeout*2, MAX_TIMEOUT)
                    self.logger.exception(err)
                    self.input_error.set()
            else:
                retry_timeout = min(retry_timeout*2, MAX_TIMEOUT)
                self.logger.debug('No message, delaying: %s', retry_timeout)
        except Exception as err:
            self.logger.exception(err)
            self.input_error.set()
            respawn = False
        finally:
            if respawn:
                self.loop.call_later(retry_timeout.total_seconds(),
                                     self.onInput,
                                     retry_timeout)


@implementer(ISource)
class StreamSource(object):
    """Implementation of ISource that reads data from stdin.
    """

    def __init__(self, logger, loop, queue, stream,
                 metric_prefix='source'):
        self.gate = queue
        self.collector = stream
        self.logger = logger
        self.loop = loop
        self.metric_prefix = metric_prefix
        self.end_of_input = Event()
        self.input_error = Event()
        self.state = RUNNING
        self.sender_tag = 'sender:%s.%s' % (self.__class__.__module__,
                                            self.__class__.__name__)
        self.loop.spawn_callback(self.onInput)

    @gen.coroutine
    def close(self):
        self.state = CLOSING
        self.logger.warning('Closing source')
        self.collector.close()

    @gen.coroutine
    def onInput(self):
        respawn = True
        try:
            msg = self.collector.readline()
            if msg:
                yield self.gate.put(msg)
                statsd.increment('%s.queued' % self.metric_prefix,
                                 tags=[self.sender_tag])
            else:
                self.end_of_input.set()
                respawn = False
        except Exception as err:
            self.logger.exception(err)
            self.input_error.set()
            respawn = False
        finally:
            if respawn:
                self.loop.spawn_callback(self.onInput)


@implementer(ISource)
class ZMQSource(object):
    """Implementation of ISource that receives messages from a ZMQ socket.
    """

    def __init__(self, logger, loop, queue, zmq_stream,
                 metric_prefix='source'):
        self.gate = queue
        self.collector = zmq_stream
        self.logger = logger
        self.loop = loop
        self.metric_prefix = metric_prefix
        self.end_of_input = Event()
        self.input_error = Event()
        self.state = RUNNING
        self.sender_tag = 'sender:%s.%s' % (self.__class__.__module__,
                                            self.__class__.__name__)
        # NOTE: this overwrites any current callback attached to the stream
        self.collector.on_recv(self._trampoline)

    def _trampoline(self, frames):
        """Bounce incoming messages to be handled properly by the IOLoop.
        """
        self.loop.add_callback(self.onInput, frames)

    @gen.coroutine
    def close(self):
        self.state = CLOSING
        self.logger.warning('Closing source')
        self.collector.close()

    @gen.coroutine
    def onInput(self, frames):
        # This will apply backpressure by not accepting input
        # until there is space in the queue.
        # This works because pyzmq uses Tornado to read from the socket;
        # reading from the socket will be blocked while the queue is full.
        yield self.gate.put(frames)
        statsd.increment('%s.queued' % self.metric_prefix,
                         tags=[self.sender_tag])
