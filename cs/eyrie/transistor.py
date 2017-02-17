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
from cs.eyrie.config import INITIAL_TIMEOUT, MAX_TIMEOUT
from cs.eyrie.interfaces import ITransistor
from tornado import gen
from zope.interface import implementer


RUNNING, CLOSING, CLOSED = range(3)


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
