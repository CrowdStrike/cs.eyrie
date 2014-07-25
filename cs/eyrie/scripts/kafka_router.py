# Copyright (C) 2014 CrowdStrike, Inc. and contributors
# This file is subject to the terms and conditions of the BSD License.
# See the file LICENSE in the main directory for details
import argparse
import logging

from setproctitle import setproctitle

import zmq
from zmq.devices import ThreadDevice

from cs.eyrie.scripts.kafka_consumer import Ranger
from cs.eyrie.config import setup_logging


def main(inbound=Ranger.output.endpoint,
         outbound='ipc:///tmp/kafka_consume.ipc'):
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
    pargs = parser.parse_args()

    if pargs.title is not None:
        setproctitle(pargs.title)

    setup_logging(pargs.config)

    logger = logging.getLogger('eyrie.kafka_router')
    logger.info('Starting Kafka feed router')

    td = ThreadDevice(zmq.STREAMER, zmq.PULL, zmq.PUSH)
    td.bind_in(inbound)
    td.bind_out(outbound)
    td.start()
    td.join()

    logger.info('Kafka feed router exiting')


if __name__ == "__main__":
    main()
