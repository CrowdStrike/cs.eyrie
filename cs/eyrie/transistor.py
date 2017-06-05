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
from collections import deque, namedtuple
from datetime import datetime
from os import linesep

import zmq
from confluent_kafka import KafkaError
from cs.eyrie.config import INITIAL_TIMEOUT, MAX_TIMEOUT
from cs.eyrie.interfaces import IDrain, IGate, IKafka, ISource, ITransistor
from datadog import statsd
from tornado import gen
from tornado.concurrent import Future, is_future
from tornado.ioloop import PeriodicCallback
from tornado.locks import Event
from tornado.queues import Queue, QueueEmpty, QueueFull
from zope.interface import implementer


DEFAULT_TRANSDUCER_CONCURRENCY = 1
RUNNING, CLOSING, CLOSED = range(3)
# See: https://github.com/confluentinc/confluent-kafka-python/issues/147
TRANSIENT_ERRORS = set([KafkaError._ALL_BROKERS_DOWN, KafkaError._TRANSPORT])


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
    incoming messages. Also reports throughput metrics.
    """

    def __init__(self, logger, loop, drain, queue, transducer, **kwargs):
        self.logger = logger
        self.loop = loop
        self.drain = drain
        self.metric_prefix = kwargs.get('metric_prefix', 'gate')
        self.transducer = transducer
        try:
            test = self.transducer(None)
            self._coroutine_transducer = is_future(test)
        except:
            self._coroutine_transducer = False
        self.state = RUNNING
        self._delay = kwargs.pop('delay', 0)
        self._queue = queue
        self._throughput_tracker = ThroughputTracker(logger, loop, **kwargs)
        self.transducer_concurrency = kwargs.get('transducer_concurrency',
                                                 DEFAULT_TRANSDUCER_CONCURRENCY)
        for i in range(self.transducer_concurrency):
            self.loop.spawn_callback(self._poll)

    @gen.coroutine
    def _operate(self, completed, incoming_msg):
        if self._coroutine_transducer:
            outgoing_msg_future = self.transducer(incoming_msg)
            self.loop.add_future(outgoing_msg_future, self._maybe_send)
        else:
            outgoing_msg = self.transducer(incoming_msg)

        if outgoing_msg is None:
            self.logger.debug('No outgoing message; dropping')
        elif isinstance(outgoing_msg, list):
            self._send(*outgoing_msg)
        else:
            self._send(outgoing_msg)
        completed.set_result(None)

    @gen.coroutine
    def _poll(self):
        """Infinite coroutine for draining the queue.
        """
        while True:
            try:
                completed, incoming_msg = self._queue.get_nowait()
            except QueueEmpty:
                self.logger.debug('Source queue empty, waiting...')
                completed, incoming_msg = yield self._queue.get()

            yield self._operate(completed, incoming_msg)

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
        completed = Future()
        self._queue.put_nowait((completed, msg))
        return completed

    @gen.coroutine
    def put(self, msg, timeout=None):
        completed = Future()
        yield self._queue.put((completed, msg), timeout)
        raise gen.Return(completed)


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
    def put(self, msg, retry_timeout=INITIAL_TIMEOUT):
        outgoing_msg = self.transducer(msg)
        self._increment()
        if outgoing_msg is None:
            self.logger.debug('No outgoing message; dropping')
        else:
            yield self.drain.emit(outgoing_msg, retry_timeout)


# Drain implementations
@implementer(IDrain)
class QueueDrain(object):
    """Implementation of IDrain that writes to a tornado.queues.Queue.
    """

    def __init__(self, logger, loop, queue,
                 metric_prefix='emitter'):
        self.emitter = queue
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
        self.emitter.put_nowait(msg)

    @gen.coroutine
    def emit(self, msg, timeout=None):
        yield self.emitter.put(msg, timeout)


@implementer(IDrain, IKafka)
class RDKafkaDrain(object):
    """Implementation of IDrain that produces to a Kafka topic using librdkafka
    asynchronously. Backpressure is implemented with a tornado.queues.Queue.
    Expects an instance of confluent_kafka.Producer as self.sender.
    """

    def __init__(self, logger, loop, producer, topic, **kwargs):
        self.emitter = producer
        self.logger = logger
        self.loop = loop
        self.loop.spawn_callback(self._poll)
        self._completed = Queue()
        self._ignored_errors = set(kwargs.get('ignored_errors', []))
        self._ignored_errors.update(TRANSIENT_ERRORS)
        self.metric_prefix = kwargs.get('metric_prefix', 'emitter')
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
            raise QueueFull

    @gen.coroutine
    def emit(self, msg, retry_timeout=INITIAL_TIMEOUT):
        while True:
            try:
                self.emit_nowait(msg)
            except QueueFull:
                yield gen.sleep(retry_timeout.total_seconds())
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
            if err.code() in self._ignored_errors:
                self.logger.warning('Ignoring error: %s', err)
            else:
                self.logger.error('Error encountered, giving up: %s', err)
                self.output_error.set()

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
    """Implementation of IDrain that pushes to a zmq.Socket asynchronously.
    This implementation overrides the high-water mark behavior from
    cs.eyrie.vassal.Vassal to instead use a zmq.Poller.
    """

    def __init__(self, logger, loop, zmq_socket,
                 metric_prefix='emitter'):
        self.emitter = zmq_socket
        self.logger = logger
        self.loop = loop
        self.metric_prefix = metric_prefix
        self.output_error = Event()
        self.state = RUNNING
        self._writable = Event()
        self.sender_tag = 'sender:%s.%s' % (self.__class__.__module__,
                                            self.__class__.__name__)

    def _handle_events(self, fd, events):
        if events & self.loop.ERROR:
            self.logger.error('Error polling socket for writability')
        elif events & self.loop.WRITE:
            self.loop.remove_handler(self.emitter)
            self._writable.set()

    @gen.coroutine
    def _poll(self):

        self.loop.add_handler(self.emitter,
                              self._handle_events,
                              self.loop.WRITE)
        yield self._writable.wait()
        self._writable.clear()

    @gen.coroutine
    def close(self):
        self.state = CLOSING
        self.logger.debug("Flushing send queue")
        self.emitter.close()

    def emit_nowait(self, msg):
        self.logger.debug("Drain emitting")
        if isinstance(msg, basestring):
            msg = [msg]
        try:
            self.emitter.send_multipart(msg, zmq.NOBLOCK)
        except zmq.Again:
            raise QueueFull

    @gen.coroutine
    def emit(self, msg, retry_timeout=INITIAL_TIMEOUT):
        if isinstance(msg, basestring):
            msg = [msg]
        yield self._poll()
        self.emitter.send_multipart(msg, zmq.NOBLOCK)


# Source implementations
@implementer(ISource)
class PailfileSource(object):
    """Implementation of ISource that reads data from a Hadoop pailfile.
    """

    def __init__(self, logger, loop, gate, sequence_reader,
                 metric_prefix='source', infinite=False):
        self.gate = gate
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


@implementer(ISource)
class QueueSource(object):
    """Implementation of ISource that reads data from a tornado.queues.Queue.
    """

    def __init__(self, logger, loop, gate, queue,
                 metric_prefix='source'):
        self.gate = gate
        self.collector = queue
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
    def close(self, timeout=None):
        self.state = CLOSING
        self.logger.warning('Joining queue')
        yield self.collector.join(timeout)

    @gen.coroutine
    def onInput(self):
        respawn = True
        try:
            try:
                msg = self.collector.get_nowait()
            except QueueEmpty:
                msg = yield self.collector.get()
            self.gate.put_nowait(msg)
        except QueueFull:
            yield self.gate.put(msg)
        except Exception as err:
            self.logger.exception(err)
            self.input_error.set()
            respawn = False
        finally:
            statsd.increment('%s.queued' % self.metric_prefix,
                             tags=[self.sender_tag])
            if respawn:
                self.loop.spawn_callback(self.onInput)


@implementer(ISource, IKafka)
class RDKafkaSource(object):
    """Implementation of ISource that consumes messages from a Kafka topic.
    """

    max_unyielded = 100000

    def __init__(self, logger, loop, gate, consumer,
                 *topics, **kwargs):
        self.gate = gate
        self.collector = consumer
        self.logger = logger
        self.loop = loop
        self.metric_prefix = kwargs.get('metric_prefix', 'source')
        self.end_of_input = Event()
        self.input_error = Event()
        self.state = RUNNING
        self.sender_tag = 'sender:%s.%s' % (self.__class__.__module__,
                                            self.__class__.__name__)
        self.loop.spawn_callback(self.onInput)
        self.collector.subscribe(list(topics))
        self._ignored_errors = set(kwargs.get('ignored_errors', []))
        self._ignored_errors.update(TRANSIENT_ERRORS)
        self.loop.spawn_callback(self.onInput)

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
        iterations = 0
        while True:
            iterations += 1
            try:
                msg = self.collector.poll(0)
                if msg is not None:
                    err = msg.error()
                    if not err:
                        retry_timeout = INITIAL_TIMEOUT
                        kafka_msg = KafkaMessage(
                            msg.key(),
                            msg.offset(),
                            msg.partition(),
                            msg.topic(),
                            msg.value(),
                        )
                        self.gate.put_nowait(kafka_msg)
                    elif err.code() in self._ignored_errors:
                        self.logger.warning('Ignoring error: %s', err)
                    else:
                        retry_timeout = min(retry_timeout*2, MAX_TIMEOUT)
                        self.logger.exception(err)
                        self.input_error.set()
                        respawn = False
                else:
                    retry_timeout = min(retry_timeout*2, MAX_TIMEOUT)
                    self.logger.debug('No message, delaying: %s', retry_timeout)
            except QueueFull:
                self.logger.debug('Gate queue full; yielding')
                yield self.gate.put(kafka_msg)
            except Exception as err:
                self.logger.exception(err)
                self.input_error.set()
                respawn = False
            finally:
                if respawn:
                    if retry_timeout > INITIAL_TIMEOUT:
                        yield gen.sleep(retry_timeout.total_seconds())
                    elif self.gate.transducer_concurrency > 1:
                        yield gen.moment
                    elif iterations > self.max_unyielded:
                        yield gen.moment
                        iterations = 0
                else:
                    break


@implementer(ISource)
class StreamSource(object):
    """Implementation of ISource that reads data from stdin.
    """

    def __init__(self, logger, loop, gate, stream,
                 metric_prefix='source'):
        self.gate = gate
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

    max_unyielded = 100000

    def __init__(self, logger, loop, queue, zmq_socket,
                 metric_prefix='source'):
        self.gate = queue
        self.collector = zmq_socket
        self.logger = logger
        self.loop = loop
        self.metric_prefix = metric_prefix
        self.end_of_input = Event()
        self.input_error = Event()
        self.state = RUNNING
        self._readable = Event()
        self.sender_tag = 'sender:%s.%s' % (self.__class__.__module__,
                                            self.__class__.__name__)
        self.loop.spawn_callback(self.onInput)

    def _handle_events(self, fd, events):
        if events & self.loop.ERROR:
            self.logger.error('Error polling socket for writability')
        elif events & self.loop.READ:
            self.loop.remove_handler(self.collector)
            self._readable.set()

    @gen.coroutine
    def _poll(self, retry_timeout=INITIAL_TIMEOUT):
        self.loop.add_handler(self.collector,
                              self._handle_events,
                              self.loop.READ)
        yield self._readable.wait()
        self._readable.clear()

    @gen.coroutine
    def close(self):
        self.state = CLOSING
        self.logger.warning('Closing source')
        self.collector.close()

    @gen.coroutine
    def onInput(self):
        # This will apply backpressure by not accepting input
        # until there is space in the queue.
        # This works because pyzmq uses Tornado to read from the socket;
        # reading from the socket will be blocked while the queue is full.
        respawn = True
        iterations = 0
        while True:
            iterations += 1
            try:
                msg = self.collector.recv_multipart(zmq.NOBLOCK)
                self.gate.put_nowait(msg)
            except zmq.Again:
                yield self._poll()
            except QueueFull:
                self.logger.debug('Gate queue full; yielding')
                yield self.gate.put(msg)
            except Exception as err:
                self.logger.exception(err)
                self.input_error.set()
                respawn = False
            else:
                statsd.increment('%s.queued' % self.metric_prefix,
                                 tags=[self.sender_tag])
            finally:
                if respawn:
                    if self.gate.transducer_concurrency > 1:
                        yield gen.moment
                    elif iterations > self.max_unyielded:
                        yield gen.moment
                        iterations = 0
                else:
                    break
