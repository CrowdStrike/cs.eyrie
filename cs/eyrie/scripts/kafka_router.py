# Copyright (C) 2014 CrowdStrike, Inc. and contributors
# This file is subject to the terms and conditions of the BSD License.
# See the file LICENSE in the main directory for details
import argparse
import logging
import sys

import zmq
from zmq.devices import ThreadProxy

from cs.eyrie.scripts.kafka_consumer import Ranger
from cs.eyrie.config import ROUTER_MONITOR, ROUTER_OUTBOUND, setup_logging


def main(inbound=Ranger.output.endpoint,
         outbound=ROUTER_OUTBOUND,
         monitor=ROUTER_MONITOR):
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config',
                        help='Path to config file',
                        required=True)
    parser.add_argument('-t', '--title',
                        help='Set the running process title',
                        default='(kafka:router)')
    parser.add_argument('--consumer-socket',
                        help='Path to socket for downstream consumers',
                        default=outbound)
    parser.add_argument('--monitor-socket',
                        help='Path to socket for monitoring',
                        default=monitor)
    parser.add_argument('--sample',
                        action='store_const',
                        help="Activate sampling mode. NOTE: must be coupled with --sample in kafka_consumer",
                        required=False,
                        default=False,
                        const=True)
    pargs = parser.parse_args()

    if pargs.title is not None:
        if '__pypy__' not in sys.builtin_module_names:
            from setproctitle import setproctitle
            setproctitle(pargs.title)

    setup_logging(pargs.config)

    logger = logging.getLogger('eyrie.kafka_router')
    logger.info('Starting Kafka feed router')

    tp = ThreadProxy(zmq.SUB if pargs.sample else zmq.PULL,
                     zmq.PUSH, zmq.PUB)

    if pargs.sample:
        tp.setsockopt_in(zmq.SUBSCRIBE, '')

    tp.bind_in(inbound)
    tp.bind_out(pargs.consumer_socket)
    tp.bind_mon(pargs.monitor_socket)
    tp.start()
    tp.join()

    logger.info('Kafka feed router exiting')


if __name__ == "__main__":
    main()
