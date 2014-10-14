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
import traceback

from pyramid.settings import asbool

from setproctitle import setproctitle

import zmq
from zmq.eventloop import ioloop


LOGGING_PORT = 16385
LOGGING_ENDPOINT = 'tcp://127.0.0.1:{}'.format(LOGGING_PORT)


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


class LogMessageHandler(object):

    __metaclass__ = ABCMeta

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
            elif 'time' in bname:
                bval = int(val)
            else:
                bval = val
            parsed_dogpile_args['behaviors'][bname] = bval
        elif 'time' in key:
            parsed_dogpile_args[key] = int(val)
        elif key == 'url':
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


def script_main(script_class, cache_region, loop=None):

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
                      context=vassal.context)

    def term_signal_handler(signal, frame):
        vassal.logger.info("%s has stopped processing messages",
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
    vassal.loop.start()

    return vassal
