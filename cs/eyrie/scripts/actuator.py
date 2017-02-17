import sys

from cs.eyrie import Vassal, script_main
from cs.eyrie.interfaces import IKafka
from cs.eyrie.transistor import (
    CLOSED,
    StreamSource,
    Transistor,
    StreamDrain,
)
from pyramid.path import DottedNameResolver
from tornado import gen
from tornado.locks import Semaphore
from tornado.queues import Queue


class Actuator(Vassal):
    channels = dict(
        Vassal.channels,
    )
    title = "(rf:actuator)"
    app_name = 'rf'
    args = [
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

        if kwargs['output'] == '-':
            del self.channels['output']
            drain = self.init_stream_drain(**kwargs)

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
