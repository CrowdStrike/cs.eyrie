import zmq
from collections import namedtuple
from cs.eyrie.config import INITIAL_TIMEOUT, MAX_TIMEOUT
from cs.eyrie.interfaces import IKafka, ISource
from cs.eyrie.transistor import CLOSING, RUNNING, TRANSIENT_ERRORS
from datadog import statsd
from tornado import gen
from tornado.locks import Event
from tornado.queues import Queue, QueueEmpty, QueueFull
from zope.interface import implementer


KafkaMessage = namedtuple(
    'KafkaMessage', [
        'key',
        'offset',
        'partition',
        'topic',
        'value',
    ],
)


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
                self.logger.info('PailfileSource queued message')
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
        while respawn:
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


@implementer(ISource)
class SQSSource(object):
    """Implementation of ISource that receives messages from a SQS queue.
    """

    max_delete_delay = 5

    def __init__(self, logger, loop, gate, sqs_client,
                 metric_prefix='source'):
        self.gate = gate
        self.collector = sqs_client
        self.logger = logger
        self.loop = loop
        self.metric_prefix = metric_prefix
        self.end_of_input = Event()
        self.input_error = Event()
        self.state = RUNNING
        self._delete_queue = Queue()
        self._flush_handle = None
        self.sender_tag = 'sender:%s.%s' % (self.__class__.__module__,
                                            self.__class__.__name__)
        self.loop.spawn_callback(self.onInput)
        self.loop.spawn_callback(self._onDelete)

    @gen.coroutine
    def close(self):
        self.state = CLOSING
        self.logger.warning('Closing source')
        yield self._flush_delete_batch()

    @gen.coroutine
    def _flush_delete_batch(self):
        qsize = self._delete_queue.qsize()
        if not qsize:
            return
        delete_batch = [
            self._delete_queue.get_nowait()
            for pos in range(qsize)
        ]
        response = yield self.collector.delete_message_batch(*delete_batch)
        if response.Failed:
            for req in response.Failed:
                self.logger.error('Message failed to delete: %s', req.Id)
            self.input_error.set()
        if self._flush_handle is not None:
            self.loop.remove_timeout(self._flush_handle)
            self._flush_handle = None

    @gen.coroutine
    def _onDelete(self):
        while True:
            qsize = self._delete_queue.qsize()
            if qsize >= self.collector.max_messages:
                yield self._flush_delete_batch()
            elif qsize and self._flush_handle is None:
                handle = self.loop.call_later(self.max_delete_delay,
                                              self._flush_delete_batch)
                self._flush_handle = handle
            yield gen.sleep(self.max_delete_delay)

    @gen.coroutine
    def onInput(self):
        respawn = True
        while respawn:
            try:
                response = yield self.collector.receive_message_batch()
                for position, msg in enumerate(response.Messages):
                    try:
                        self.gate.put_nowait(msg)
                    except QueueFull:
                        self.logger.debug('Gate queue full; yielding')
                        yield self.gate.put(msg)
                    self._delete_queue.put_nowait(msg)
                    statsd.increment('%s.queued' % self.metric_prefix,
                                     tags=[self.sender_tag])
            except Exception as err:
                self.logger.exception(err)
                self.input_error.set()
                respawn = False


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
        while respawn:
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
