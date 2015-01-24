# Copyright (C) 2014 CrowdStrike, Inc. and contributors
# This file is subject to the terms and conditions of the BSD License.
# See the file LICENSE in the main directory for details
import argparse
import logging

from setproctitle import setproctitle

import zmq
from zmq.devices import ThreadProxy

from cs.eyrie.scripts.kafka_consumer import Ranger
from cs.eyrie.config import setup_logging


def main(inbound=Ranger.output.endpoint,
         outbound='ipc:///tmp/kafka_consume.ipc',
         monitor='ipc:///tmp/kafka_monitor.ipc'):
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
    pargs = parser.parse_args()

    if pargs.title is not None:
        setproctitle(pargs.title)

    setup_logging(pargs.config)

    logger = logging.getLogger('eyrie.kafka_router')
    logger.info('Starting Kafka feed router')

    tp = ThreadProxy(zmq.PULL, zmq.PUSH, zmq.PUB)
    tp.bind_in(inbound)
    tp.bind_out(pargs.consumer_socket)
    tp.bind_mon(pargs.monitor_socket)
    tp.start()
    tp.join()

    logger.info('Kafka feed router exiting')


if __name__ == "__main__":
    main()
