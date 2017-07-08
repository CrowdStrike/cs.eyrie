import zmq
from collections import namedtuple
from cs.eyrie.config import INITIAL_TIMEOUT, MAX_TIMEOUT
from cs.eyrie.interfaces import IDrain, IKafka
from cs.eyrie.transistor import CLOSED, CLOSING, RUNNING, TRANSIENT_ERRORS
from datetime import datetime
from os import linesep
from tornado import gen
from tornado.locks import Event
from tornado.queues import Queue, QueueFull
from zope.interface import implementer


RoutingMessage = namedtuple(
    'RoutingMessage', [
        'destination',
        'value',
    ],
)


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
        # See: https://github.com/confluentinc/confluent-kafka-python/issues/147
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
class RoutingDrain(object):
    """Implementation of IDrain that pushes to named drain(s) asynchronously.
    Destination is determined by the message.
    """

    def __init__(self, logger, loop, **kwargs):
        self.metric_prefix = kwargs.pop('metric_prefix', 'emitter')
        self.emitter = {
            destination: drain
            for destination, drain in kwargs.items()
        }
        self.logger = logger
        self.loop = loop
        self.output_error = Event()
        self.state = RUNNING
        self.sender_tag = 'sender:%s.%s' % (self.__class__.__module__,
                                            self.__class__.__name__)

    @gen.coroutine
    def close(self):
        self.state = CLOSING
        self.logger.debug("Flushing send queue")
        drain_futures = [
            drain.close()
            for drain in self.emitter.values()
        ]
        yield gen.multi(drain_futures)

    def emit_nowait(self, msg):
        self.logger.debug("RoutingDrain emitting")
        assert isinstance(msg, RoutingMessage)
        self.emitter[msg.destination].emit_nowait(msg.value)

    @gen.coroutine
    def emit(self, msg, retry_timeout=INITIAL_TIMEOUT):
        assert isinstance(msg, RoutingMessage)
        yield self.emitter[msg.destination].emit(msg.value, retry_timeout)


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
