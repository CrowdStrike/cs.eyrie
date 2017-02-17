import sys

import zmq
from cs.eyrie import Vassal, ZMQChannel, script_main
from cs.eyrie.interfaces import IKafka
from cs.eyrie.transistor import (
    CLOSED,
    StreamSource, ZMQSource,
    Transistor,
    StreamDrain, ZMQDrain,
)
from pyramid.path import DottedNameResolver
from tornado import gen
from tornado.locks import Semaphore
from tornado.queues import Queue


class Actuator(Vassal):
    channels = dict(
        Vassal.channels,
        input=ZMQChannel(
            # This is configured dynamically at runtime
            endpoint=None,
            socket_type=zmq.PULL,
        ),
        output=ZMQChannel(
            # This is configured dynamically at runtime
            endpoint=None,
            socket_type=zmq.PUSH,
        ),
    )
    title = "(rf:actuator)"
    app_name = 'rf'
    args = [
        (
            ('--bind-input',),
            dict(
                help="Bind ZMQ input socket",
                default=True,
                action='store_true',
            )
        ),
        (
            ('--bind-output',),
            dict(
                help="Bind ZMQ output socket",
                default=False,
                action='store_true',
            )
        ),
        (
            ('--input',),
            dict(
                help="Source to be used as input",
                required=True,
            )
        ),
        (
            ('--output',),
            dict(
                help="Destination of input data",
                required=True,
            )
        ),
        (
            ('--inflight',),
            dict(
                help="Maximum number of messages to keep in flight",
                required=False,
                default=500,
                type=int,
            )
        ),
        (
            ('--queued',),
            dict(
                help="Maximum number of messages to queue",
                required=False,
                default=1000,
                type=int,
            )
        ),
        (
            ('--transducer',),
            dict(
                help="Dotted-path to function to transform input messages to output",
                default='cs.eyrie.transistor.get_last_element',
            )
        ),
    ]

    def __init__(self, **kwargs):
        kwargs['init_db'] = False
        kwargs['init_streams'] = False
        self.streams = {}
        super(Actuator, self).__init__(**kwargs)
        self.transistor = self.init_transistor(**kwargs)

    def init_stream_drain(self, **kwargs):
        return StreamDrain(
            self.logger,
            self.loop,
            Semaphore(kwargs['inflight']),
            sys.stdout,
        )

    def init_stream_source(self, **kwargs):
        return StreamSource(
            self.logger,
            self.loop,
            kwargs['gate'],
            sys.stdin,
        )

    def init_transistor(self, **kwargs):
        # The gate is shared between the transistor and source
        kwargs['gate'] = Queue(kwargs['queued'])
        if kwargs['input'][0] == '-':
            del self.channels['input']
            source = self.init_stream_source(**kwargs)
        elif '://' in kwargs['input'][0]:
            source = self.init_zmq_source(**kwargs)

        if kwargs['output'] == '-':
            del self.channels['output']
            drain = self.init_stream_drain(**kwargs)
        elif '://' in kwargs['output'][0]:
            drain = self.init_zmq_drain(**kwargs)

        resolver = DottedNameResolver()
        transducer = resolver.maybe_resolve(kwargs['transducer'])
        return Transistor(
            self.logger,
            self.loop,
            kwargs['gate'],
            source,
            drain,
            transducer,
        )

    def init_zmq_drain(self, **kwargs):
        channel = ZMQChannel(**dict(
            vars(self.channels['output']),
            bind=kwargs['bind_output'],
            endpoint=kwargs['output'],
        ))
        socket = self.context.socket(channel.socket_type)
        socket.set_hwm(kwargs['inflight'])
        if kwargs['bind_output']:
            socket.bind(kwargs['output'])
        else:
            socket.connect(kwargs['output'])
        return ZMQDrain(
            self.logger,
            self.loop,
            socket,
        )

    def init_zmq_source(self, **kwargs):
        channel = ZMQChannel(**dict(
            vars(self.channels['input']),
            bind=kwargs['bind_input'],
            endpoint=kwargs['input'][0],
        ))
        socket = self.context.socket(channel.socket_type)
        socket.set_hwm(kwargs['inflight'])
        if kwargs['bind_input']:
            socket.bind(kwargs['input'][0])
        else:
            socket.connect(kwargs['input'][0])
        return ZMQSource(
            self.logger,
            self.loop,
            kwargs['gate'],
            socket,
        )

    @gen.coroutine
    def join(self, timeout=None):
        yield self.transistor.join(timeout)
        yield self.terminate()

    @gen.coroutine
    def terminate(self):
        if self.transistor.state != CLOSED:
            self.transistor.close('Actuator terminating')
        super(Actuator, self).terminate()


def main():
    actuator = script_main(Actuator, None, start_loop=False)
    actuator.join()
    actuator.loop.start()


if __name__ == "__main__":
    main()
