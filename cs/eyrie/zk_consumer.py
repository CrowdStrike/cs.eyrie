# Copyright (C) 2014 CrowdStrike, Inc. and contributors
# This file is subject to the terms and conditions of the BSD License.
# See the file LICENSE in the main directory for details
from __future__ import absolute_import
from collections import defaultdict
from functools import partial
import json
import logging
import os
import socket
import time

try:
    from kafka.client import KafkaClient
    from kafka.common import FailedPayloadsError
    from kafka.consumer import SimpleConsumer

    from kazoo.client import KazooClient
    from kazoo.exceptions import KazooException
    from kazoo.exceptions import NodeExistsError
    from kazoo.protocol.states import KazooState
    from kazoo.recipe.party import ShallowParty
    from kazoo.retry import KazooRetry
    from kazoo.retry import RetryFailedError
    from kazoo.recipe.watchers import PatientChildrenWatch
    from kazoo.recipe.partitioner import PartitionState
except ImportError:
    KafkaClient = None
    FailedPayloadsError = None
    SimpleConsumer = None
    KazooClient = None
    KazooException = None
    NodeExistsError = None
    KazooState = None
    ShallowParty = None
    KazooRetry = None
    RetryFailedError = None
    PatientChildrenWatch = None
    PartitionState = None


# https://cwiki.apache.org/confluence/display/KAFKA/Kafka+data+structures+in+Zookeeper
class ZKPartitioner(object):
    """Claims partitions in a Kafka topic.

    When the :class:`ZKPartitioner` enters the
    :attr:`~PartitionState.FAILURE` state, it is unrecoverable
    and a new :class:`ZKPartitioner` should be created.

    Example:

    .. code-block:: python

        from kazoo.client import KazooClient
        client = KazooClient()

        qp = ZKPartitioner(client, consumer_group, topic)

        while 1:
            if qp.failed:
                raise Exception("Lost or unable to acquire partition")
            elif qp.release:
                qp.release_set()
            elif qp.acquired:
                for partition in qp:
                    # Do something with each partition
            elif qp.allocating:
                qp.wait_for_acquire()

    **State Transitions**

    When created, the :class:`ZKPartitioner` enters the
    :attr:`PartitionState.ALLOCATING` state.

    :attr:`~PartitionState.ALLOCATING` ->
    :attr:`~PartitionState.ACQUIRED`

        Set was partitioned successfully, the partition list assigned
        is accessible via list/iter methods or calling list() on the
        :class:`ZKPartitioner` instance.

    :attr:`~PartitionState.ALLOCATING` ->
    :attr:`~PartitionState.FAILURE`

        Allocating the set failed either due to a Zookeeper session
        expiration, or failure to acquire the items of the set within
        the timeout period.

    :attr:`~PartitionState.ACQUIRED` ->
    :attr:`~PartitionState.RELEASE`

        The members of the consumer group have changed, and the set needs to be
        repartitioned. :meth:`ZKPartitioner.release` should be called
        as soon as possible.

    :attr:`~PartitionState.ACQUIRED` ->
    :attr:`~PartitionState.FAILURE`

        The current partition was lost due to a Zookeeper session
        expiration.

    :attr:`~PartitionState.RELEASE` ->
    :attr:`~PartitionState.ALLOCATING`

        The current partition was released and is being re-allocated.

    """

    path_formats = {
        'owner': '/consumers/{group}/owners/{topic}/{partition}',
        'group': '/consumers/{group}/ids',
        'topic': '/brokers/topics/{topic}/partitions',
    }

    def __init__(self, client, group, topic,
                 identifier=None, time_boundary=30,
                 partitions_changed_cb=None, logger=None):
        """Create a :class:`~ZKPartitioner` instance

        :param client: A :class:`~kazoo.client.KazooClient` instance.
        :param identifier: An identifier to use for this member of the
                           consumer group when participating. Defaults to the
                           hostname + process id.
        :param time_boundary: How long the consumer group members must be stable
                              before allocation can complete.

        """
        if logger is None:
            logger = logging.getLogger('kafka.consumer.zkpartitioner')
        self.logger = logger
        if PartitionState is None:
            raise RuntimeError("ZooKeeper support requires cs.eyrie to be installed with the Kafka extra: install_requires= ['cs.eyrie[Kafka]']")
        self.state = PartitionState.ALLOCATING
        self.group = group
        self.topic = topic
        self.consumer_partitions = defaultdict(list)

        self._locks = []
        self._client = client
        self._identifier = identifier or '{}-{}'.format(
            socket.getfqdn(), os.getpid()
        )
        self._group_path = self.path_formats['group'].format(group=group)

        self._set = []
        self._client.handler.spawn(self.get_partitions)

        self._time_boundary = time_boundary

        self._acquire_event = client.handler.event_object()

        self.join_group()

        self._was_allocated = False
        self._state_change = client.handler.rlock_object()
        client.add_listener(self._establish_sessionwatch)

        # Now watch the group and set the callback on the async result
        # so we know when we're ready
        self._children_updated = False
        self._child_watching(self._allocate_transition, async=True)
        self.partitions_changed_cb = partitions_changed_cb

    def get_partitions(self):
        t_partition_path = self.path_formats['topic'].format(topic=self.topic)
        self._set = self._client.get_children(t_partition_path)

    def __iter__(self):
        """Return the partitions in this partition set"""
        for partition in self.consumer_partitions[self._identifier]:
            yield partition

    @property
    def failed(self):
        """Corresponds to the :attr:`PartitionState.FAILURE` state"""
        return self.state == PartitionState.FAILURE

    @property
    def release(self):
        """Corresponds to the :attr:`PartitionState.RELEASE` state"""
        return self.state == PartitionState.RELEASE

    @property
    def allocating(self):
        """Corresponds to the :attr:`PartitionState.ALLOCATING`
        state"""
        return self.state == PartitionState.ALLOCATING

    @property
    def acquired(self):
        """Corresponds to the :attr:`PartitionState.ACQUIRED` state"""
        return self.state == PartitionState.ACQUIRED

    def join_group(self):
        self._group = self._client.ShallowParty(self._group_path,
                                                identifier=self._identifier)
        # Mimic Scala data:
        # {"version":1,"subscription":{"foo.bar":1},"pattern":"static","timestamp":"1404263054520"}
        subscription = {
            'version': 1,
            'subscription': {self.topic: 1},
            'pattern': 'static',
            'timestamp': str(int(time.time() * 1000)),
        }
        self._group.data = json.dumps(subscription)
        self.logger.info('Joining consumer group %s as %s',
                         self._group_path, self._identifier)
        self._group.join()

    def wait_for_acquire(self, timeout=30):
        """Wait for the set to be partitioned and acquired

        :param timeout: How long to wait before returning.
        :type timeout: int

        """
        self._acquire_event.wait(timeout)

    def release_set(self):
        """Call to release the set

        This method begins the step of allocating once the set has
        been released.

        """
        self._release_locks()
        if self._locks:  # pragma: nocover
            # This shouldn't happen, it means we couldn't release our
            # locks, abort
            self._fail_out()
            return
        else:
            with self._state_change:
                if self.failed:
                    return
                self.state = PartitionState.ALLOCATING
        self._child_watching(self._allocate_transition, async=True)

    def finish(self):
        """Call to release the set and leave the consumer group"""
        self._release_locks()
        self._fail_out()

    def _fail_out(self):
        with self._state_change:
            self.state = PartitionState.FAILURE
        if self._group.participating:
            try:
                self.logger.warn('Leaving consumer group: %s ( %s )',
                                 self._group_path, self._identifier)
                self._group.leave()
            except KazooException:  # pragma: nocover
                pass

    def _allocate_transition(self, result):
        """Called when in allocating mode, and the children settled"""
        # Did we get an exception waiting for children to settle?
        if result.exception:  # pragma: nocover
            self._fail_out()
            return

        children, async_result = result.get()
        self._children_updated = False

        # Add a callback when children change on the async_result
        def updated(result):
            with self._state_change:
                if self.acquired:
                    self.state = PartitionState.RELEASE
            self._children_updated = True

        async_result.rawlink(updated)

        # Proceed to acquire locks for the working set as needed
        self.rebalance(self._set)

        # All locks acquired! Time for state transition, make sure
        # we didn't inadvertently get lost thus far
        with self._state_change:
            if self.failed:  # pragma: nocover
                return self.finish()
            self.state = PartitionState.ACQUIRED
            self.logger.info('All partitions acquired: %d',
                             len(self.consumer_partitions[self._identifier]))
            self._acquire_event.set()

    def _release_locks(self):
        """Attempt to completely remove all the locks"""
        self._acquire_event.clear()

        my_partitions = self.consumer_partitions[self._identifier]
        nodes = sorted([node for node in self._group])
        my_old_partitions = [
            partition
            for partition in self._set
            if nodes[int(partition) % len(nodes)] != self._identifier and
                int(partition) in my_partitions
        ]
        self.logger.debug('Release locks: my old partitions: %r', my_old_partitions)
        for partition in my_old_partitions:
            self.logger.info('Releasing ownership of partition %s',
                             partition)
            p_path = self.path_formats['owner'].format(group=self.group,
                                                       topic=self.topic,
                                                       partition=partition)
            self.logger.info('Deleting path: %s', p_path)
            self._client.delete(p_path)
            if int(partition) in my_partitions:
                self.logger.info('Removing from my partitions: %s', partition)
                my_partitions.remove(int(partition))

    def _abort_lock_acquisition(self):
        """Called during lock acquisition if a consumer group change occurs"""
        self._partition_set = []
        self._release_locks()
        if self._locks:
            # This shouldn't happen, it means we couldn't release our
            # locks, abort
            self._fail_out()
            return
        return self._child_watching(self._allocate_transition)

    def _child_watching(self, func=None, async=False):
        """Called when children are being watched to stabilize

        This actually returns immediately, child watcher spins up a
        new thread/greenlet and waits for it to stabilize before
        any callbacks might run.

        """
        self.watcher = PatientChildrenWatch(self._client, self._group_path,
                                            self._time_boundary)
        asy = self.watcher.start()
        if func is not None:
            # We spin up the function in a separate thread/greenlet
            # to ensure that the rawlink's it might use won't be
            # blocked
            if async:
                func = partial(self._client.handler.spawn, func)
            asy.rawlink(func)
        return asy

    def _establish_sessionwatch(self, state):
        """Register ourself to listen for session events, we shut down
        if we become lost"""
        with self._state_change:
            # Handle network partition: If connection gets suspended,
            # change state to ALLOCATING if we had already ACQUIRED. This way
            # the caller does not process the members since we could eventually
            # lose session get repartitioned. If we got connected after a suspension
            # it means we've not lost the session and still have our members. Hence,
            # restore to ACQUIRED
            if state == KazooState.SUSPENDED:
                if self.state == PartitionState.ACQUIRED:
                    self._was_allocated = True
                    self.state = PartitionState.ALLOCATING
            elif state == KazooState.CONNECTED:
                if self._was_allocated:
                    self._was_allocated = False
                    self.state = PartitionState.ACQUIRED

        if state == KazooState.LOST:
            self._client.handler.spawn(self._fail_out)
            return True

    def rebalance(self, partition_ids=None):
        if partition_ids is None:
            partition_ids = [
                str(p_id)
                for p_id in self.consumer_partitions[self._identifier]
            ]
        kr = KazooRetry(max_tries=3)
        kr.retry_exceptions = kr.retry_exceptions + tuple([NodeExistsError])

        my_partitions = self.consumer_partitions[self._identifier]
        self.logger.info('My partitions (%d): %s', len(my_partitions), my_partitions)

        # Clean up old ownership data first, so we don't block
        # the joining node(s)
        self._release_locks()

        nodes = sorted([node for node in self._group])
        my_new_partitions = [
            partition
            for partition in partition_ids
            if nodes[int(partition) % len(nodes)] == self._identifier and
               int(partition) not in my_partitions
        ]
        self.logger.info('My new partitions (%d): %s', len(my_new_partitions), my_new_partitions)
        for partition in my_new_partitions:
            c_id = nodes[int(partition) % len(nodes)]
            self.consumer_partitions[c_id].append(int(partition))
            p_path = self.path_formats['owner'].format(group=self.group,
                                                       topic=self.topic,
                                                       partition=partition)
            try:
                self.logger.debug('Acquiring ownership of partition %s',
                                  partition)
                kr(self._client.create, p_path,
                   value=self._identifier, ephemeral=True, makepath=True)
            except RetryFailedError as err:
                # A different consumer had been registered as the owner
                expired_cid, zstat = self._client.get(p_path)
                msg = 'Acquiring ownership of partition %s (was owned by %s)'
                self.logger.warn(msg, partition, expired_cid)
                # We need to delete / create, so that the node is created
                # ephemeral and owned by us
                self._client.delete(p_path)
                self._client.create(p_path, value=self._identifier,
                                    ephemeral=True, makepath=True)
        if self.partitions_changed_cb:
            self.partitions_changed_cb(self.consumer_partitions[self._identifier])


class StaticShallowParty(ShallowParty):

    def __init__(self, client, path, nodes, identifier=None):
        self._nodes = nodes
        super(StaticShallowParty, self).__init__(client, path, identifier)

    def _get_children(self):
        return self._nodes


class StaticZKPartitioner(ZKPartitioner):

    def __init__(self, client, group, topic, nodes,
                 identifier=None, time_boundary=0,
                 partitions_changed_cb=None, logger=None):
        self._nodes = nodes
        if identifier is None:
            identifier = socket.getfqdn()
        super(StaticZKPartitioner, self).__init__(client, group, topic,
                                                  identifier, time_boundary,
                                                  partitions_changed_cb, logger)

    def join_group(self):
        self._group = StaticShallowParty(self._client, self._group_path,
                                         self._nodes, self._identifier)
        # Mimic Scala data:
        # {"version":1,"subscription":{"foo.bar":1},"pattern":"static","timestamp":"1404263054520"}
        subscription = {
            'version': 1,
            'subscription': {self.topic: 1},
            'pattern': 'static',
            'timestamp': str(int(time.time() * 1000)),
        }
        self._group.data = json.dumps(subscription)
        self.logger.info('Joining consumer group %s as %s',
                         self._group_path, self._identifier)
        self._group.join()


class ZKConsumer(object):

    zk_timeout = 30
    jitter_seconds = 30
    broker_prefix = '/brokers/ids'

    def __init__(
            self,
            zk_hosts,
            group,
            topic,
            nodes,
            zk_handler=None,
            logger=None,
            **consumer_kwargs):
        """Creates a Consumer that tracks state in ZooKeeper,
        rebalancing partition ownership as registered consumers change.
        NOTE: this class is intended for version 0.8.1 of Kafka, where offsets
              are managed by Kafka but there is no rebalancing in the protocol.
        """
        if logger is None:
            logger = logging.getLogger('kafka.consumer.ZKConsumer')
        self.logger = logger

        if KafkaClient is None:
            raise RuntimeError("Kafka support requires cs.eyrie to be installed with the Kafka extra: install_requires= ['cs.eyrie[Kafka]']")
        self.zk_handler = zk_handler
        self.zk_hosts = zk_hosts
        self.broker_hosts = []

        self.group = group
        self.topic = topic

        self.zk = None
        self.nodes = nodes
        self.client = None
        self.consumer = None
        self.consumer_kwargs = consumer_kwargs

        # This will kick off a cascading sequence to initialize ourselves:
        # 1. Connect to ZK and pull list of Kafka brokers
        # 2. Register ourselves as a consumer in ZK
        # 3. Rebalance partitions across all connected consumers
        self.init_zk()

    def zk_session_watch(self, state):
        self.logger.debug('ZK transitioned to: %s', state)
        if state == KazooState.SUSPENDED:
            if self.consumer is not None:
                self.logger.info('Stopping Kafka consumer')
                self.consumer.stop()
                self.consumer = None
            # Lost connection to ZK; we can't call any methods that would
            # try to contact it (i.e., we can't do self.zkp.finish() )
            self.zkp = None
        elif state == KazooState.CONNECTED:
            self.logger.info('Restarting ZK partitioner')
            self.zk.handler.spawn(self.init_zkp)

    def _zkp_wait(self):
        while 1:
            if self.zkp.failed:
                raise Exception("Lost or unable to acquire partition")
            elif self.zkp.release:
                self.zkp.release_set()
            elif self.zkp.acquired:
                def group_change_proxy(event):
                    self.logger.warn('Connected consumers changed')
                    if self.zkp is None or self.zkp.failed:
                        self.logger.info('Restarting ZK partitioner')
                        self.zk.handler.spawn(self.init_zkp)
                    else:
                        self.logger.info('ZK partitioner releasing set')
                        self.zkp.release_set()
                        self.logger.info('Re-joining group')
                        self.zk.handler.spawn(self.zkp.join_group)
                if not self.nodes:
                    self.logger.info('Partitioner aquired; setting child watch')
                    result = self.zk.get_children_async(self.zkp._group_path)
                    result.rawlink(group_change_proxy)
                break
            elif self.zkp.allocating:
                self.zkp.wait_for_acquire()

    def init_zkp(self):
        if self.nodes:
            self.zkp = StaticZKPartitioner(
                self.zk, self.group, self.topic, self.nodes,
                partitions_changed_cb=self.init_consumer,
                logger=self.logger)
        else:
            self.zkp = ZKPartitioner(
                self.zk, self.group, self.topic,
                time_boundary=self.jitter_seconds,
                partitions_changed_cb=self.init_consumer,
                logger=self.logger)

        self._zkp_wait()

    def init_zk(self):
        # TODO: switch to async
        # 1. implement kazoo.interfaces.IHandler in terms of Tornado's IOLoop
        self.zk = KazooClient(hosts=self.zk_hosts, handler=self.zk_handler)
        self.zk.start()
        self.zk.add_listener(self.zk_session_watch)

        @self.zk.ChildrenWatch(self.broker_prefix)
        def broker_change_proxy(broker_ids):
            self.onBrokerChange(broker_ids)

        self.init_zkp()

    def onBrokerChange(self, broker_ids):
        self.broker_hosts = []
        for b_id in broker_ids:
            b_json, zstat = self.zk.get('/'.join([self.broker_prefix, b_id]))
            b_data = json.loads(b_json)
            self.broker_hosts.append('{}:{}'.format(b_data['host'],
                                                    b_data['port']))

        my_partitions = []
        if self.consumer is not None:
            self.logger.warn('Brokers changed, stopping Kafka consumer.')
            my_partitions = self.consumer.offsets.keys()
            self.consumer.stop()
            self.consumer = None
        if self.client is not None:
            self.logger.warn('Brokers changed, stopping Kafka client.')
            self.client.close()
            self.client = None

        if my_partitions:
            msg = 'Brokers changed, queuing restart of Kafka client / consumer.'
            self.logger.warn(msg)
            self.zk.handler.spawn(self.init_consumer, my_partitions)

    def init_consumer(self, my_partitions):
        if self.consumer is None:
            self.logger.warn('Starting Kafka client')
            self.client = KafkaClient(self.broker_hosts,
                                      client_id=self.zkp._identifier)
        else:
            if self.consumer is None or \
               sorted(my_partitions) != sorted(self.consumer.offsets.keys()):
                self.logger.warn('Partitions changed, restarting Kafka consumer.')
                self.consumer.stop()
            else:
                self.logger.info('Partitions unchanged, not restarting Kafka consumer.')
                return

        self.consumer = SimpleConsumer(self.client, self.group, self.topic,
                                       partitions=my_partitions,
                                       **self.consumer_kwargs)
        self.consumer.provide_partition_info()
        self.logger.info("Consumer connected to Kafka: %s", self.consumer.offsets)

    def stop(self):
        if self.consumer is not None:
            self.logger.info('Stopping Kafka consumer')
            self.consumer.stop()
            self.consumer = None
        if self.client is not None:
            self.logger.info('Stopping Kafka client')
            self.client.close()
            self.client = None
        if self.zk is not None:
            self.logger.info('Stopping ZooKeeper client')
            self.zkp.finish()
            self.zkp = None
            self.zk.stop()
            self.zk = None

    def commit(self, partitions=None):
        """
        Commit offsets for this consumer

        partitions: list of partitions to commit, default is to commit
                    all of them
        """
        if self.consumer is None:
            return
        self.logger.debug('Begin committing offsets for partitions: %s',
                          partitions if partitions else 'All')
        self.consumer.commit(partitions)
        self.logger.debug('End committing offsets for partitions: %s',
                          partitions if partitions else 'All')

    def pending(self, partitions=None):
        """
        Gets the pending message count

        partitions: list of partitions to check for, default is to check all
        """
        return self.consumer.pending(partitions)

    def provide_partition_info(self):
        """
        Indicates that partition info must be returned by the consumer
        """
        self.consumer.provide_partition_info()

    def seek(self, offset, whence):
        """
        Alter the current offset in the consumer, similar to fseek

        offset: how much to modify the offset
        whence: where to modify it from
                0 is relative to the earliest available offset (head)
                1 is relative to the current offset
                2 is relative to the latest known offset (tail)
        """
        self.consumer.seek(offset, whence)

    def get_messages(self, count=1, block=True, timeout=0.1):
        """
        Fetch the specified number of messages

        count: Indicates the maximum number of messages to be fetched
        block: If True, the API will block till some messages are fetched.
        timeout: If block is True, the function will block for the specified
                 time (in seconds) until count messages is fetched. If None,
                 it will block forever.
        """
        if self.consumer is None:
            return []
        else:
            try:
                return self.consumer.get_messages(count, block, timeout)
            except FailedPayloadsError:
                msg = 'Failed to retrieve payload, restarting consumer'
                self.logger.exception(msg)
                self.stop()
                self.init_zk()
                return []

    def get_message(self, block=True, timeout=0.1, get_partition_info=None):
        return self.consumer.get_message(block, timeout, get_partition_info)

    def _get_message(self, block=True, timeout=0.1, get_partition_info=None,
                     update_offset=True):
        return self.consumer._get_message(block, timeout, get_partition_info,
                                          update_offset)

    def __iter__(self):
        for msg in self.consumer:
            yield msg
