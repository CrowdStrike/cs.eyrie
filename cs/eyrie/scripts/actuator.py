import sys
from os.path import isfile

import zmq
from confluent_kafka import Consumer, Producer
from cs.eyrie import Vassal, ZMQChannel, script_main
from cs.eyrie.interfaces import IKafka
from cs.eyrie.transistor import (
    CLOSED,
    PailfileSource, RDKafkaSource, StreamSource, ZMQSource,
    Gate, Transistor,
    RDKafkaDrain, StreamDrain, ZMQDrain,
)
try:
    from hadoop.io import SequenceFile
except ImportError:
    SequenceFile = None
from pyramid.path import DottedNameResolver
from tornado import gen
from tornado.locks import Semaphore


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
    app_name = 'actuator'
    args = [
        # ZMQ options
        (
            ('--bind-input',),
            dict(
                help="Bind ZMQ input socket",
                default=False,
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
            ('--input-socket-type',),
            dict(
                help="Which ZMQ socket type to use for input",
                required=False,
                choices=['pull', 'sub'],
                default='pull',
            )
        ),
        (
            ('--output-socket-type',),
            dict(
                help="Which ZMQ socket type to use for output",
                required=False,
                choices=['push', 'pub'],
                default='push',
            )
        ),
        # Kafka options
        (
            ('--bootstrap-servers',),
            dict(
                help="Initial list of Kafka brokers",
                required=False,
                default='127.0.0.1:9092',
                nargs='+',
            )
        ),
        (
            ('--group-name',),
            dict(
                help="Kafka consumer group name",
                required=False,
            )
        ),
        (
            ('--offset-reset',),
            dict(
                help="Action to take when there is no initial offset in offset store or the desired offset is out of range",
                required=False,
                choices=['smallest', 'largest'],
                default='largest',
            )
        ),
        # Base options
        (
            ('--input',),
            dict(
                help="Source to be used as input",
                required=True,
                nargs='+',
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

    def init_kafka_drain(self, **kwargs):
        return RDKafkaDrain(
            self.logger,
            self.loop,
            Producer({
                'api.version.request': True,
                'bootstrap.servers': ','.join(kwargs['bootstrap_servers']),
                'default.topic.config': {'produce.offset.report': True},
                # The lambda is necessary to return control to the main Tornado
                # thread
                'error_cb': lambda err: self.loop.add_callback(self.onKafkaError,
                                                               err),
                'group.id': kwargs['group_name'],
                # See: https://github.com/edenhill/librdkafka/issues/437
                'log.connection.close': False,
                'queue.buffering.max.ms': 1000,
                'queue.buffering.max.messages': kwargs['inflight'],
            }),
            kwargs['output'],
        )

    def init_kafka_source(self, **kwargs):
        return RDKafkaSource(
            self.logger,
            self.loop,
            kwargs['gate'],
            Consumer({
                'api.version.request': True,
                'bootstrap.servers': ','.join(kwargs['bootstrap_servers']),
                #'debug': 'all',
                'default.topic.config': {
                    'auto.offset.reset': kwargs['offset_reset'],
                    'enable.auto.commit': True,
                    'offset.store.method': 'broker',
                    'produce.offset.report': True,
                },
                'enable.partition.eof': False,
                # The lambda is necessary to return control to the main Tornado
                # thread
                'error_cb': lambda err: self.loop.add_callback(self.onKafkaError,
                                                               err),
                'group.id': kwargs['group_name'],
                # See: https://github.com/edenhill/librdkafka/issues/437
                'log.connection.close': False,
                'max.in.flight': kwargs['inflight'],
                'queue.buffering.max.ms': 1000,
            }),
            *kwargs['input']
        )

    def init_pailfile_source(self, **kwargs):
        return PailfileSource(
            self.logger,
            self.loop,
            kwargs['gate'],
            SequenceFile.Reader(kwargs['input'][0]),
        )

    def init_stream_drain(self, **kwargs):
        return StreamDrain(
            self.logger,
            self.loop,
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
        if kwargs['output'] == '-':
            del self.channels['output']
            drain = self.init_stream_drain(**kwargs)
        elif '://' in kwargs['output']:
            drain = self.init_zmq_drain(**kwargs)
        else:
            del self.channels['output']
            drain = self.init_kafka_drain(**kwargs)

        # The gate "has" a drain;
        # a source "has" a gate
        resolver = DottedNameResolver()
        transducer = resolver.maybe_resolve(kwargs['transducer'])
        kwargs['gate'] = Gate(
            self.logger,
            self.loop,
            drain,
            transducer,
        )
        if kwargs['input'][0] == '-':
            del self.channels['input']
            source = self.init_stream_source(**kwargs)
        elif isfile(kwargs['input'][0]):
            del self.channels['input']
            source = self.init_pailfile_source(**kwargs)
        elif '://' in kwargs['input'][0]:
            source = self.init_zmq_source(**kwargs)
        else:
            del self.channels['input']
            source = self.init_kafka_source(**kwargs)

        return Transistor(
            self.logger,
            self.loop,
            kwargs['gate'],
            source,
            drain,
        )

    def init_zmq_drain(self, **kwargs):
        channel = ZMQChannel(**dict(
            vars(self.channels['output']),
            bind=kwargs['bind_output'],
            endpoint=kwargs['output'],
            socket_type=getattr(zmq, kwargs['output_socket_type'].upper()),
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
            socket_type=getattr(zmq, kwargs['input_socket_type'].upper()),
        ))
        socket = self.context.socket(channel.socket_type)
        if channel.socket_type == zmq.SUB:
            socket.setsockopt(zmq.SUBSCRIBE, '')
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
    def onKafkaError(self, err):
        self.logger.error(err)
        if IKafka.providedBy(self.transistor.drain):
            self.transistor.drain.output_error.set()
        if IKafka.providedBy(self.transistor.source):
            self.transistor.source.input_error.set()

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
