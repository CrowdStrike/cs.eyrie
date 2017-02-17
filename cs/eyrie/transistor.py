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
from os import linesep

from cs.eyrie.config import INITIAL_TIMEOUT, MAX_TIMEOUT
from cs.eyrie.interfaces import IDrain, ISource, ITransistor
from datadog import statsd
from tornado import gen
from tornado.locks import Event
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


def get_last_element(msg):
    if isinstance(msg, (list, tuple)):
        return msg[-1]
    return msg


@implementer(ITransistor)
class Transistor(object):
    """Implementation of ITransistor that handles all interaction with
    the queue. This class is responsible for applying backpressure when
    the configured emitter is slower than the collector.
    """

    def __init__(self, logger, loop, queue, source, drain, transducer,
                 metric_prefix='transistor'):
        self.logger = logger
        self.loop = loop
        self.gate = queue
        # Queue's start with this set; we want to wait until we've started
        # to receive input
        self.gate._finished.clear()
        self.source = source
        self.drain = drain
        # spawn_callback doesn't associate with the current stack context
        self.loop.spawn_callback(self.onDrain)
        self.metric_prefix = metric_prefix
        self.transducer = transducer
        self._max_inflight = self.drain.inflight._value
        self.state = RUNNING

    @gen.coroutine
    def close(self, msg_prefix):
        try:
            self.state = CLOSING
            msg = '%s; closing source queued: %d inflight: %d'
            self.logger.error(msg,
                              msg_prefix,
                              self.gate.qsize(),
                              self._max_inflight-self.drain.inflight._value)
            yield self.source.close()
            if not self.gate.empty():
                msg = '%s; draining queue queued: %d inflight: %d'
                self.logger.error(msg,
                                  msg_prefix,
                                  self.gate.qsize(),
                                  self._max_inflight-self.drain.inflight._value)
                try:
                    yield self.gate.join(MAX_TIMEOUT)
                except gen.TimeoutError:
                    msg = '%s; unable to drain queue queued: %d inflight: %d'
                    self.logger.error(msg,
                                      msg_prefix,
                                      self.gate.qsize(),
                                      self._max_inflight-self.drain.inflight._value)
            msg = '%s; closing drain queued: %d inflight: %d'
            self.logger.error(msg,
                              msg_prefix,
                              self.gate.qsize(),
                              self._max_inflight-self.drain.inflight._value)
            yield self.drain.close()
        except Exception as err:
            self.logger.exception(err)
        finally:
            self.state = CLOSED

    @gen.coroutine
    def join(self, timeout=None):
        futures = dict(
            queue=self.gate.join(timeout),
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

    @gen.coroutine
    def _emit(self, msg, retry_timeout=INITIAL_TIMEOUT):
        while True:
            try:
                yield self.drain.emit(msg, retry_timeout)
                break
            except gen.TimeoutError:
                log_msg = "Transistor waiting for drain, queued: %d inflight: %d"
                self.logger.info(log_msg,
                                 self.gate.qsize(),
                                 self._max_inflight-self.drain.inflight._value)
                retry_timeout = min(retry_timeout*2, MAX_TIMEOUT)

    @gen.coroutine
    def onDrain(self, retry_timeout=INITIAL_TIMEOUT):
        respawn = True
        try:
            log_msg = "Transistor waiting for input, queued: %d inflight: %d"
            self.logger.info(log_msg,
                             self.gate.qsize(),
                             self._max_inflight-self.drain.inflight._value)
            # This pauses execution in this coroutine until item(s)
            # are present in the queue
            incoming_msg = yield self.gate.get(retry_timeout)
            retry_timeout = INITIAL_TIMEOUT
            outgoing_msg = self.transducer(incoming_msg)
            yield self._emit(outgoing_msg)
            self.gate.task_done()
        except gen.TimeoutError:
            retry_timeout = min(retry_timeout*2, MAX_TIMEOUT)
        except Exception as err:
            self.logger.exception(err)
        finally:
            if respawn:
                self.loop.spawn_callback(self.onDrain, retry_timeout)


# Drain implementations
@implementer(IDrain)
class StreamDrain(object):
    """Implementation of IDrain that writes to stdout.
    """

    def __init__(self, logger, loop, inflight, stream,
                 metric_prefix='emitter'):
        self.emitter = stream
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
        with (yield self.inflight.acquire(timeout)):
            self.logger.debug("Drain emitting")
            self.emitter.write(msg)
            if not msg.endswith(linesep):
                self.emitter.write(linesep)

    @gen.coroutine
    def onTrack(self, *args):
        """Callback interface for when messages are delivered.
        """
        pass


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
