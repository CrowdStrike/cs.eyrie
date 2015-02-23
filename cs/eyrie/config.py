# Copyright (C) 2014 CrowdStrike, Inc. and contributors
# This file is subject to the terms and conditions of the BSD License.
# See the file LICENSE in the main directory for details
from __future__ import absolute_import

from abc import ABCMeta
from abc import abstractmethod
try:
    from ConfigParser import RawConfigParser
except ImportError:
    from configparser import RawConfigParser
import argparse
from collections import namedtuple
from copy import copy
from functools import partial
import logging
from logging.config import dictConfig
import multiprocessing
import os
import signal
import sys
import traceback

from pyramid.settings import asbool

from setproctitle import setproctitle

import six

import zmq
from zmq.eventloop import ioloop

try:
    from dogpile.cache.api import CacheBackend, NO_VALUE
except ImportError:
    CacheBackend = None
    NO_VALUE = None


LOGGING_PORT = 16385
LOGGING_ENDPOINT = 'tcp://127.0.0.1:{}'.format(LOGGING_PORT)
ROUTER_OUTBOUND = 'ipc:///tmp/kafka_consume.ipc'
ROUTER_MONITOR = 'ipc:///tmp/kafka_monitor.ipc'


SOCKET_TYPES = {
    zmq.REQ: 'REQ',
    zmq.REP: 'REP',
    zmq.PUB: 'PUB',
    zmq.SUB: 'SUB',
    zmq.PAIR: 'PAIR',
    zmq.DEALER: 'DEALER',
    zmq.ROUTER: 'ROUTER',
    zmq.PUSH: 'PUSH',
    zmq.PULL: 'PULL',
}

ZMQChannel = namedtuple(
    'ZMQChannel',
    ['endpoint', 'socket_type', 'bind', 'subscription',
     'recv_handler', 'hwm', 'drained_by', 'drains'],
)
ZMQChannel.__new__ = partial(
    ZMQChannel.__new__,
    bind=False,
    subscription=['*', str(os.getpid())],
    recv_handler=None,
    # This value is additive to the ZMQ socket HWM
    hwm=1000,
    # Specifying this value will trigger HWM throttling
    drained_by=None,
    # This must be set on the next pipeline stage after drained_by
    drains=None,
)


ZMQLogMessage = namedtuple(
    'ZMQLogMessage',
    ['logger_name', 'hostname', 'logger_type', 'serialized_record',
     'version', 'digest', 'iv', 'tag', 'serialization_format']
)


DEFAULT_ITERATIONS = 100000
DEFAULT_IV_BITS = 96


@six.add_metaclass(ABCMeta)
class LogMessageHandler(object):

    @abstractmethod
    def handle(self, msg):
        return msg

    @classmethod
    def __subclasshook__(cls, C):
        if cls is LogMessageHandler:
            if any("handle" in B.__dict__ for B in C.__mro__):
                return True
        return NotImplemented


# Unfortunately, RawConfigParser forces all option keys to lower-case
class BaseConfigParser(RawConfigParser):

    def optionxform(self, optionstr):
        return optionstr


def setup_logging(config_uri, incremental=False, **kwargs):
    logging_config = {'version': 1, 'incremental': incremental}
    cp = BaseConfigParser()
    cp.read([config_uri])

    for scontainer, sbegin in {'loggers': 'logger',
                               'handlers': 'handler',
                               'formatters': 'formatter',
                               'filters': 'filter'}.items():
        section = {}
        if not cp.has_section(scontainer):
            continue
        for sname in [i.strip()
                      for i in cp.get(scontainer, 'keys').split(',')]:
            if not sname:
                continue
            settings = {}
            for skey, sval in cp.items('_'.join([sbegin, sname])):
                if ',' in sval:
                    sval = [i.strip() for i in sval.split(',')]
                if skey in ('handlers', 'filters') and \
                   not isinstance(sval, list):
                    if sval:
                        sval = [sval]
                    else:
                        sval = []
                elif skey in ('backupCount', 'maxBytes'):
                    sval = int(sval)
                elif skey == 'stream':
                    sval = logging.config._resolve(sval)
                elif skey == 'args':
                    continue
                elif skey == 'propagate':
                    sval = asbool(sval)
                settings[skey] = sval
            section[sname] = settings

        logging_config[scontainer] = section

    root_config = logging_config['loggers'].pop('root')

    if 'zmq' in root_config['handlers'] and 'context' in kwargs:
        logging_config['handlers']['zmq']['context'] = kwargs['context']
        logging_config['handlers']['zmq']['loop'] = kwargs['loop']
        logging_config['handlers']['zmq']['async'] = kwargs.get('async', False)

    logging_config['root'] = root_config

    dictConfig(logging_config)

    if incremental:
        logging.info('Logging re-configured.')
    else:
        logging.info('Logging configured.')


def configure_caching(cache_region, config_uri):
    try:
        from dogpile import cache
    except ImportError:
        raise RuntimeError("Dogpile support requires cs.eyrie to be installed with the Dogpile extra: install_requires= ['cs.eyrie[Dogpile]']")

    cp = RawConfigParser()
    cp.read([config_uri])
    if cp.has_section('dogpile'):
        dogpile_args = dict(cp.items('dogpile'))
    else:
        dogpile_args = {}
    parsed_dogpile_args = {}
    parsed_dogpile_args['behaviors'] = {}
    for key, val in dogpile_args.items():
        if key.startswith('behavior'):
            bname = key.split('.')[1]
            if bname in ('ketama', 'ketama_weighted', 'buffer_requests',
                         'cache_lookups', 'no_block', 'tcp_nodelay', 'cas',
                         'verify_keys', 'remove_failed', 'distributed_lock'):
                bval = asbool(val)
            elif 'time' in bname or bname == '_noreply':
                bval = int(val)
            else:
                bval = val
            parsed_dogpile_args['behaviors'][bname] = bval
        elif 'time' in key:
            parsed_dogpile_args[key] = int(val)
        elif key == 'url':
            if dogpile_args['backend'] == 'dogpile.cache.redis':
                parsed_dogpile_args['url'] = val
            else:
                parsed_dogpile_args['url'] = val.split(',')
        elif key == 'binary':
            parsed_dogpile_args['binary'] = asbool(val)
        elif key == 'min_compress_len':
            parsed_dogpile_args['min_compress_len'] = int(val)
        else:
            parsed_dogpile_args[key] = val

    cache_region.configure(
        parsed_dogpile_args.pop('backend'),
        expiration_time=parsed_dogpile_args.pop('expiration_time'),
        arguments=parsed_dogpile_args)


def info_signal_handler(signal, frame):
    curr_proc = multiprocessing.current_process()
    logger = logging.getLogger('eyrie.script.stats')
    logger.error('Dumping stack for: %s, PID: %d\n%s',
                 curr_proc.name, curr_proc.pid,
                 ''.join(traceback.format_stack(frame)))


def script_main(script_class, cache_region, loop=None, start_loop=True):

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config',
                        help='Path to config file',
                        required=True)

    parser.add_argument('-t', '--title',
                        help='Set the running process title',
                        default=script_class.title)

    blt = 'Logs a stack trace if the IOLoop is blocked for more than s seconds'
    parser.add_argument('--blocking-log-threshold',
                        help=blt, default=5, type=int, metavar='s')

    if script_class.args is not None:
        for pa, kw in script_class.args:
            parser.add_argument(*pa, **kw)

    pargs = parser.parse_args()

    if pargs.title is not None:
        setproctitle(pargs.title)

    # TODO: add signal handlers to drop caches
    signal.signal(signal.SIGUSR1, info_signal_handler)

    # Pop off kwargs not relevant to script class
    kwargs = copy(vars(pargs))
    kwargs['context'] = zmq.Context()
    kwargs['async'] = True
    if loop is None:
        loop = ioloop.IOLoop.instance()
    kwargs['loop'] = loop
    for a in ['blocking_log_threshold']:
        kwargs.pop(a)

    if cache_region is not None:
        configure_caching(cache_region, pargs.config)

    setup_logging(pargs.config, **kwargs)
    vassal = script_class(**kwargs)

    def hup_signal_handler(signal, frame):
        setup_logging(vassal.config_uri, incremental=True,
                      context=vassal.context, loop=vassal.loop, async=True)

    def term_signal_handler(signal, frame):
        vassal.logger.info("%s has received terminate signal",
                           script_class.__name__)
        root = logging.getLogger()
        for handler in root.handlers:
            if hasattr(handler, 'stream'):
                handler.stream.flush()
        vassal.terminate()

    signal.signal(signal.SIGHUP, hup_signal_handler)
    signal.signal(signal.SIGINT, term_signal_handler)
    signal.signal(signal.SIGTERM, term_signal_handler)

    vassal.loop.set_blocking_log_threshold(pargs.blocking_log_threshold)
    vassal.loop.add_callback(vassal.logger.info,
                             "%s has begun processing messages",
                             script_class.__name__)
    if start_loop:
        try:
            vassal.loop.start()
        except zmq.Again:
            print 'Terminating with unsent messages'

    return vassal


class ShardedRedisBackend(CacheBackend):
    """A backend to shard keys across a Redis cluster
    """

    def __init__(self, arguments):
        self._imports()
        self.distributed_lock = arguments.get('distributed_lock', False)
        self.lock_timeout = arguments.get('lock_timeout', None)
        self.lock_sleep = arguments.get('lock_sleep', 0.1)

        self.cluster = ShardedRedis(
            shards=[
                host.split(':')
                for host in arguments['url']
            ],
            duration=arguments.pop('redis_expiration_time', 0),
            hashfn=arguments.pop('hashfn', None),
            db=arguments.pop('db', 0),
            password=arguments.pop('password', None),
            socket_timeout=arguments.pop('socket_timeout', None),
            charset=arguments.pop('charset', 'utf-8'),
            errors=arguments.pop('errors', 'strict'),
        )

    def _imports(self):
        # defer imports until backend is used
        global ShardedRedis
        from cs.eyrie.redis import ShardedRedis

    def get_mutex(self, key):
        if self.distributed_lock:
            return self.cluster.lock('_lock{0}'.format(key),
                                     timeout=self.lock_timeout,
                                     sleep=self.lock_sleep)
        else:
            return None

    def get(self, key):
        value = self.cluster.get(key)
        if value is None:
            return NO_VALUE
        return pickle.loads(value)

    def get_multi(self, keys):
        values = self.cluster.mget(keys)
        return [
            pickle.loads(v)
            if v is not None else NO_VALUE
            for v in values
        ]

    def set(self, key, value):
        self.cluster.set(key, pickle.dumps(value, pickle.HIGHEST_PROTOCOL))

    def set_multi(self, mapping):
        mapping = dict(
            (k, pickle.dumps(v, pickle.HIGHEST_PROTOCOL))
            for k, v in mapping.items()
        )
        self.cluster.mset(mapping)

    def delete(self, key):
        self.cluster.delete(key)

    def delete_multi(self, keys):
        self.cluster.delete(*keys)
