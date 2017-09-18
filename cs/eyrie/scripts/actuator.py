import sys
import urlparse as urlparse_module
from urlparse import parse_qs, urlparse, urlunparse

import zmq
from botocore.session import get_session
from cs.eyrie import Vassal, ZMQChannel, script_main
from cs.eyrie.interfaces import IKafka
from cs.eyrie.transistor import (
    CLOSED, TRANSIENT_ERRORS,
    AsyncSQSClient,
    PailfileSource, RDKafkaSource, SQSSource, StreamSource, ZMQSource,
    Gate, Transistor,
    RDKafkaDrain, SQSDrain, StreamDrain, ZMQDrain,
)
try:
    from hadoop.io import SequenceFile
except ImportError:
    SequenceFile = None
from pyramid.path import DottedNameResolver
from pyramid.settings import asbool, aslist
from tornado import gen
from tornado.httpclient import AsyncHTTPClient


# http://api.zeromq.org/4-2:zmq-connect
ZMQ_TRANSPORTS = {
    'epgm',
    'inproc',
    'ipc',
    'pgm,'
    'tcp',
    'vmci',
}


def _register_scheme(scheme):
    for method in filter(lambda s: s.startswith('uses_'),
                         dir(urlparse_module)):
        getattr(urlparse_module, method).append(scheme)


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
        (
            ('--input',),
            dict(
                help="Source to be used as input",
                required=True,
                nargs='+',
                type=urlparse,
            )
        ),
        (
            ('--output',),
            dict(
                help="Destination of input data",
                required=True,
                type=urlparse,
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
        (
            ('--transducer-config',),
            dict(
                help="Arguments passed to transducer at startup",
                default=[],
                required=False,
                nargs='*',
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
        from confluent_kafka import Producer
        params = parse_qs(kwargs['output'].query)
        bootstrap_servers = params['bootstrap_servers']
        list_bootstrap_servers = aslist(bootstrap_servers[0].replace(',', ' '))
        if len(list_bootstrap_servers) > 1:
            bootstrap_servers = list_bootstrap_servers
        else:
            bootstrap_servers = params['bootstrap_servers']

        return RDKafkaDrain(
            self.logger,
            self.loop,
            Producer({
                'api.version.request': True,
                'bootstrap.servers': ','.join(bootstrap_servers),
                'default.topic.config': {'produce.offset.report': True},
                # The lambda is necessary to return control to the main Tornado
                # thread
                'error_cb': lambda err: self.loop.add_callback(self.onKafkaError,
                                                               err),
                'group.id': params['group_name'][-1],
                # See: https://github.com/edenhill/librdkafka/issues/437
                'log.connection.close': False,
                'queue.buffering.max.ms': 1000,
                'queue.buffering.max.messages': kwargs['inflight'],
            }),
            kwargs['output'].netloc,
        )

    def init_kafka_source(self, **kwargs):
        from confluent_kafka import Consumer
        params = {}
        for parsed_url in kwargs['input']:
            url_params = parse_qs(parsed_url.query)
            for key, val in url_params.items():
                params.setdefault(key, []).extend(val)

        bootstrap_servers = params['bootstrap_servers']
        list_bootstrap_servers = aslist(bootstrap_servers[0].replace(',', ' '))
        if len(list_bootstrap_servers) > 1:
            bootstrap_servers = list_bootstrap_servers
        else:
            bootstrap_servers = params['bootstrap_servers']

        offset_reset = params.get('offset_reset')
        if offset_reset:
            offset_reset = offset_reset[-1]
        else:
            offset_reset = 'largest'

        strategy = params.get('partition_strategy')
        if strategy:
            strategy = strategy[-1]
        else:
            strategy = 'roundrobin'

        return RDKafkaSource(
            self.logger,
            self.loop,
            kwargs['gate'],
            Consumer({
                'api.version.request': True,
                'bootstrap.servers': ','.join(bootstrap_servers),
                #'debug': 'all',
                'default.topic.config': {
                    'auto.offset.reset': offset_reset,
                    'enable.auto.commit': True,
                    'offset.store.method': 'broker',
                    'produce.offset.report': True,
                },
                'enable.partition.eof': False,
                # The lambda is necessary to return control to the main Tornado
                # thread
                'error_cb': lambda err: self.loop.add_callback(self.onKafkaError,
                                                               err),
                'group.id': params['group_name'][0],
                # See: https://github.com/edenhill/librdkafka/issues/437
                'log.connection.close': False,
                'max.in.flight': kwargs['inflight'],
                'partition.assignment.strategy': strategy,
                'queue.buffering.max.ms': 1000,
            }),
            *[url.netloc for url in kwargs['input']]
        )

    def init_pailfile_source(self, **kwargs):
        return PailfileSource(
            self.logger,
            self.loop,
            kwargs['gate'],
            SequenceFile.Reader(kwargs['input'][0].path),
        )

    def _init_sqs_client(self, parsed_url, **kwargs):
        params = parse_qs(parsed_url.query)
        session = get_session()
        queue_url = params.get('queue_url')
        if queue_url:
            queue_url = queue_url[-1]
        else:
            queue_url = None

        region = params.get('region')
        if region:
            region = region[-1]
        else:
            region = None

        return AsyncSQSClient(
            session,
            self.logger,
            queue_name=parsed_url.netloc,
            queue_url=queue_url,
            region=region,
            http_client=AsyncHTTPClient(
                self.loop,
                force_instance=True,
                defaults=dict(
                    request_timeout=AsyncSQSClient.long_poll_timeout+5,
                )
            )
        )

    def init_sqs_drain(self, **kwargs):
        sqs_client = self._init_sqs_client(kwargs['output'], **kwargs)
        return SQSDrain(
            self.logger,
            self.loop,
            sqs_client,
        )

    def init_sqs_source(self, **kwargs):
        sqs_client = self._init_sqs_client(kwargs['input'][0], **kwargs)
        return SQSSource(
            self.logger,
            self.loop,
            kwargs['gate'],
            sqs_client,
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
        if kwargs['output'].scheme == 'file' and \
           kwargs['output'].netloc == '-':
            del self.channels['output']
            drain = self.init_stream_drain(**kwargs)
        elif kwargs['output'].scheme.lower() in ZMQ_TRANSPORTS:
            drain = self.init_zmq_drain(**kwargs)
        elif kwargs['output'].scheme == 'kafka':
            del self.channels['output']
            drain = self.init_kafka_drain(**kwargs)
        elif kwargs['output'].scheme == 'sqs':
            del self.channels['output']
            drain = self.init_sqs_drain(**kwargs)
        else:
            raise ValueError(
                'Unsupported drain scheme: {}'.format(kwargs['output'].scheme)
            )

        # The gate "has" a drain;
        # a source "has" a gate
        resolver = DottedNameResolver()
        transducer = resolver.maybe_resolve(kwargs['transducer'])
        if kwargs['transducer_config']:
            transducer = transducer(*kwargs['transducer_config'])
        kwargs['gate'] = Gate(
            self.logger,
            self.loop,
            drain,
            transducer,
        )

        if not kwargs['input'][0].scheme and kwargs['input'][0].path == '-':
            del self.channels['input']
            source = self.init_stream_source(**kwargs)
        elif kwargs['input'][0].scheme == 'file':
            del self.channels['input']
            source = self.init_pailfile_source(**kwargs)
        elif kwargs['input'][0].scheme.lower() in ZMQ_TRANSPORTS:
            source = self.init_zmq_source(**kwargs)
        elif kwargs['input'][0].scheme == 'kafka':
            del self.channels['input']
            source = self.init_kafka_source(**kwargs)
        elif kwargs['input'][0].scheme == 'sqs':
            del self.channels['input']
            source = self.init_sqs_source(**kwargs)
        else:
            raise ValueError(
                'Unsupported source scheme: {}'.format(kwargs['input'].scheme)
            )

        return Transistor(
            self.logger,
            self.loop,
            kwargs['gate'],
            source,
            drain,
        )

    def _init_zmq_socket(self, parsed_url, channel, **kwargs):
        # Reconstruct ZMQ endpoint, sans query parameters
        endpoint = urlunparse((parsed_url.scheme,
                               parsed_url.netloc,
                               parsed_url.path,
                               None, None, None))
        params = parse_qs(parsed_url.query)
        bind = params.get('bind')
        if bind:
            bind = asbool(bind[0])
        else:
            bind = False
        socket_type = params.get('socket_type')
        if socket_type:
            socket_type = socket_type[0]
        else:
            socket_type = kwargs['default_socket_type']
        channel = ZMQChannel(**dict(
            vars(channel),
            bind=bind,
            endpoint=endpoint,
            socket_type=getattr(zmq, socket_type.upper()),
        ))
        socket = self.context.socket(channel.socket_type)
        if channel.socket_type == zmq.SUB:
            socket.setsockopt(zmq.SUBSCRIBE, '')
        socket.set_hwm(kwargs['inflight'])
        if bind:
            socket.bind(endpoint)
        else:
            socket.connect(endpoint)
        return socket

    def init_zmq_drain(self, **kwargs):
        kwargs['default_socket_type'] = 'push'
        socket = self._init_zmq_socket(kwargs['output'],
                                       self.channels['output'],
                                       **kwargs)
        return ZMQDrain(
            self.logger,
            self.loop,
            socket,
        )

    def init_zmq_source(self, **kwargs):
        kwargs['default_socket_type'] = 'pull'
        socket = self._init_zmq_socket(kwargs['input'][0],
                                       self.channels['input'],
                                       **kwargs)
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
        if err.code() in TRANSIENT_ERRORS:
            self.logger.warning('Ignoring: %s', err)
        else:
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
    # Execute this before script_main, to avoid polluting on simple module
    # import but also to be present before argparse does its thing
    _register_scheme('kafka')
    _register_scheme('sqs')
    for scheme in ZMQ_TRANSPORTS:
        _register_scheme(scheme)

    actuator = script_main(Actuator, None, start_loop=False)
    actuator.join()
    actuator.loop.start()


if __name__ == "__main__":
    main()
