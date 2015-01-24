# Copyright (C) 2014 CrowdStrike, Inc. and contributors
# This file is subject to the terms and conditions of the BSD License.
# See the file LICENSE in the main directory for details
from __future__ import absolute_import
import argparse
import logging
import multiprocessing
import random
import time

import gevent

from kafka.common import KafkaMessage

from kazoo.handlers.gevent import SequentialGeventHandler
from kazoo.protocol.states import KazooState

from pyramid.config import Configurator
from pyramid.exceptions import ConfigurationError
from pyramid.paster import get_appsettings
from pyramid.config import aslist

from setproctitle import setproctitle

import zmq.green as zmq

from cs.eyrie.config import setup_logging
from cs.eyrie.config import ZMQChannel
from cs.eyrie.zk_consumer import ZKConsumer


class Ranger(object):
    output = ZMQChannel(
        endpoint='ipc:///tmp/kafka_feed.ipc',
        socket_type=zmq.PUSH,
    )

    buffer_size = 65535*4
    commit_interval = 30
    fetch_count = 25
    max_buffer_size = 1024*1024
    title = '(kafka:consumer)'

    def __init__(self, config_uri, app_name,
                 zk_hosts=None, group=None, topic=None):
        self.config_uri = config_uri
        self.curr_proc = multiprocessing.current_process()

        setup_logging(self.config_uri)
        app_settings = get_appsettings(self.config_uri, name=app_name)
        self.config = Configurator(settings=app_settings)
        self.curr_proc.authkey = self.config.registry.settings['eyrie.authkey']

        self.msg_count = 0
        self.logger = logging.getLogger('rf.kafka')

        settings = self.config.registry.settings
        self.commit_interval = int(settings.get('kafka.commit_interval',
                                                self.commit_interval))
        self.commit_greenlet = None
        self.commit_spawn_greenlet = None
        self.consume_greenlet = None
        self.fetch_count = int(settings.get('kafka.fetch_count',
                                            self.fetch_count))

        zk_hosts = settings.get('kafka.zk_hosts', zk_hosts)
        if zk_hosts is None:
            raise ConfigurationError('No ZooKepper hosts provided')
        group = settings.get('kafka.group', group)
        if group is None:
            raise ConfigurationError('No consumer group provided to join')
        topic = settings.get('kafka.topic', topic)
        if topic is None:
            raise ConfigurationError('No topic provided to consume')
        consumers = settings.get('kafka.consumers', None)
        if consumers:
            consumers = aslist(consumers)

        self.consumer = ZKConsumer(
            zk_hosts,
            group,
            topic,
            zk_handler=SequentialGeventHandler(),
            logger=self.logger,
            buffer_size=int(settings.get('kafka.buffer_size',
                                         self.buffer_size)),
            max_buffer_size=int(settings.get('kafka.max_buffer_size',
                                             self.max_buffer_size)),
            auto_commit=False,
            nodes=consumers,
        )
        self.consumer.zk.add_listener(self.zk_session_watch)

        self.context = zmq.Context()
        self.channel = self.context.socket(self.output.socket_type)
        self.channel.connect(self.output.endpoint)

    def __call__(self):
        commit_interval = random.choice(range(self.commit_interval-15,
                                              self.commit_interval+15))
        self.commit_spawn_greenlet = gevent.spawn_later(commit_interval,
                                                        self.onCommitSpawn)
        if self.consumer.nodes:
            self.logger.info('Beginning pull')
            self.consume_greenlet = gevent.spawn_later(0, self.onConsume)
        else:
            initial_sleep = random.choice(range(5, 15))
            self.logger.info('Sleeping for %d seconds before pulling',
                             initial_sleep)
            self.consume_greenlet = gevent.spawn_later(initial_sleep,
                                                       self.onConsume)
        hub = gevent.get_hub()
        hub.join()

    def send(self, partition, msg):
        try:
            import pdb;pdb.set_trace()
            kmsg = KafkaMessage(
                self.consumer.topic, str(partition), str(msg.offset),
                msg.message.key, msg.message.value,
            )
            self.channel.send_multipart(kmsg)
        except AssertionError:
            gevent.spawn_later(0, self.send, msg)

    def onConsume(self):
        try:
            for partition_msg in self.consumer.get_messages(self.fetch_count,
                                                            block=False):
                self.msg_count += 1
                self.send(*partition_msg)
        except Exception:
            self.logger.exception('Error encountered, restarting consumer')
            self.consumer.stop()
            self.consumer.init_zk()
        finally:
            self.consume_greenlet = gevent.spawn_later(0, self.onConsume)

    def onCommitSpawn(self):
        try:
            self.commit_greenlet = gevent.spawn(self.consumer.commit)
        except Exception:
            self.logger.exception('Error encountered while committing')
        finally:
            commit_interval = random.choice(range(self.commit_interval-15,
                                                  self.commit_interval+15))
            self.commit_spawn_greenlet = gevent.spawn_later(commit_interval,
                                                            self.onCommitSpawn)

    def zk_session_watch(self, state):
        self.logger.debug('ZK transitioned to: %s', state)
        if state == KazooState.CONNECTED:
            self.logger.info('Resuming consume greenlet')
            self.consume_greenlet = gevent.spawn_later(0, self.onConsume)
            commit_interval = random.choice(range(self.commit_interval-15,
                                                  self.commit_interval+15))
            self.logger.info('Resuming commit spawn greenlet')
            self.commit_spawn_greenlet = gevent.spawn_later(commit_interval,
                                                            self.onCommitSpawn)
        elif state == KazooState.SUSPENDED:
            if self.commit_greenlet is not None:
                self.logger.info('Killing commit greenlet')
                self.commit_greenlet = self.commit_greenlet.kill()
            if self.consume_greenlet is not None:
                self.logger.info('Killing consume greenlet')
                self.consume_greenlet = self.consume_greenlet.kill()
            if self.commit_spawn_greenlet is not None:
                self.logger.info('Killing commit spawn greenlet')
                self.commit_spawn_greenlet = self.commit_spawn_greenlet.kill()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config',
                        help='Path to config file',
                        required=True)
    parser.add_argument('--app-name',
                        help='App name to use when retrieving settings',
                        required=True)
    parser.add_argument('--zk-hosts',
                        help='Comma-separated list of ZooKeeper hosts')
    parser.add_argument('-g', '--group',
                        help='Name of Kafka consumer group to join')
    parser.add_argument('-t', '--topic',
                        help='Kafka topic to consume')
    parser.add_argument('--title',
                        help='Set the running process title',
                        default=Ranger.title)
    pargs = parser.parse_args()

    if pargs.title is not None:
        setproctitle(pargs.title)

    ranger = Ranger(config_uri=pargs.config,
                    app_name=pargs.app_name,
                    zk_hosts=pargs.zk_hosts,
                    group=pargs.group,
                    topic=pargs.topic)
    ranger()


if __name__ == "__main__":
    main()
