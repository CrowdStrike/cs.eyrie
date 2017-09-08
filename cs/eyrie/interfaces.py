# -*- coding: utf-8 -*-
"""
The terminology used for the parts is taken from transistors (specifically a
common-gate, field-effect transistor: https://en.wikipedia.org/wiki/Common_gate
).
                              ◯ Vdd
                              ┃
                            * ┃*
                         *    ┃  *
                        *     ┃   *
                        *     ┃   *  Id
                         *    V  *
                            * ┃*
                              ┃
                              ┣━━━━━━━━◯ Vout
                           D  ┃
                            * ┃*
                    G    *   |┛  *
                      ┏━*━━┫ |┓   *
                      ┃ *    |┫   *
                      ┃  *    ^  *
                    ━━┻━━   * ┃*
                     ━━━   S  ┃
                      ━       ◯ Vin

"Current source ID represents an active load; signal is applied at node Vin and
output is taken from node Vout; output can be current or voltage."

In the above diagram, the source Id represents the object implementing
ITransistor. It drives data flow by constantly reading from its input queue and
calling its IDrain `emit` method. The ISource implementation pushes messages to
the ITransistor input queue (the common `gate`).

ITransistor has two resonsiblites in regards to linking two input/output
sources of type ISource/IDrain.
    (1) Provide backpressure information from IDrain to ISource in the form of
        Tornado coroutine
    (2) Provide confirmed send thru usage of a confirmed write queue from
        IDrain
These two things allow both IDrain and ISource implementations to be only
concerned with their network protocols rather than the interop to the
source/destination protocol.
"""

from zope.interface import Attribute, Interface


class ITransistor(Interface):
    """ Wrapper object to coordinate collectors and emitters.
    """

    source = Attribute('Our configured source object')
    drain = Attribute('Our configured drain object')
    gate = Attribute('Instance of tornado.queues.Queue for input data')
    logger = Attribute('stdlib logger instance')
    loop = Attribute('Instance of the Tornado IOLoop')
    state = Attribute('Current transition state')

    def close(msg_prefix, timeout):
        """Close any open resources and exit gracefully.
        """

    def join(timeout):
        """Coroutine to wait for end of input and/or error conditions.
        """


class IGate(Interface):
    """ Wrapper object to coordinate collectors and emitters.
    """

    drain = Attribute('Our configured drain object')
    logger = Attribute('stdlib logger instance')
    loop = Attribute('Instance of the Tornado IOLoop')
    num_emitted = Attribute('Number of messages that have passed through the gate')
    samples = Attribute('Deque of ThroughputSample instances')
    state = Attribute('Current transition state')
    transducer = Attribute('Callable to transform input messages to output messages')
    transducer_concurrency = Attribute("""
        How many instances of transducer to run concurrently. If using
        a coroutine for a transducer, you'll want to increase this.
    """)

    def put_nowait():
        """Fast-path call to push a message downstream.
        """

    def put():
        """Coroutine that blocks until the configured drain accepts the message.
        """


class IDrain(Interface):
    """ An object responsible for draining coordinator input.
    """

    emitter = Attribute('Our configured emitter object')
    inflight = Attribute("Semaphore controlling how many messages we allow in flight")
    logger = Attribute('stdlib logger instance')
    loop = Attribute('Instance of the Tornado IOLoop')
    state = Attribute('Current transition state')
    sender_tag = Attribute('Tag added to all metrics pushed to DataDog')

    def close(timeout):
        """Close any open resources and exit gracefully.
        """

    def emit(msg, timeout=None):
        """Coroutine that sends a message.
        """


class ISource(Interface):
    """ An object responsible for receiving data and feeding coordinator input.
    """

    collector = Attribute('Our configured collecting object')
    gate = Attribute('Instance of tornado.queues.Queue for input data')
    logger = Attribute('stdlib logger instance')
    loop = Attribute('Instance of the Tornado IOLoop')
    sender_tag = Attribute('Tag added to all metrics pushed to DataDog')
    end_of_input = Attribute('tornado.locks.Event signifying source has reached end of input')

    def close(timeout):
        """Close any open resources and exit gracefully.
        """

    def onInput(*args):
        """Infinite coroutine responsible for calling self.gate.put() to
        push a message into the queue. Relies on configured transistor to
        push further down the circuit.
        """


class IKafka(Interface):
    """Marker interface for implementations using Kafka.
    """
