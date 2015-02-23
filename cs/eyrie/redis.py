from __future__ import absolute_import

from redis.client import StrictRedis
import zlib


class ShardedProxy(object):

    def __init__(self, shards, duration=None, hashfn=None):
        self.proxy = StrictRedis
        self.shards = shards
        self.num_shards = len(shards)
        self.duration = duration

        if not hashfn:
            hashfn = zlib.crc32

        self.hashfn = hashfn

    #### Utility methods ####
    def shard_for_key(self, key):
        return self.shards[self.hashfn(key) % self.num_shards]

    def sharded(self, key, method, *args, **kwargs):
        shard = self.shard_for_key(key)
        result = method(shard, *args, **kwargs)
        if self.duration:
            shard.expire(key, self.duration)
        return result

    def gather_keys_by_shard(self, keys):
        # Gather keys by shard
        shards = {}
        for key in keys:
            shard = self.shard_for_key(key)
            if shard in shards:
                shards[shard].append(key)
            else:
                shards[shard] = [key]
        return shards.items()

    #### Server Information ####
    def bgwriteaof(self):
        return [shard.bgwriteaof() for shard in self.shards]

    def bgsave(self):
        return [shard.bgsave() for shard in self.shards]

    def dbsize(self):
        return [shard.dbsize() for shard in self.shards]

    def delete(self, *names):
        """deletes the specified keys -- not atomic across shards."""

        return [shard.delete(*keys) for shard, keys
                in self.gather_keys_by_shard(names)]

    def flushall(self):
        return [shard.flushall() for shard in self.shards]

    def flushdb(self):
        return [shard.flushdb() for shard in self.shards]

    def info(self):
        return [shard.info() for shard in self.shards]

    def lastsave(self):
        return [shard.lastsave() for shard in self.shards]

    def ping(self):
        return [shard.ping() for shard in self.shards]

    def save(self):
        return [shard.save() for shard in self.shards]

    #### Basic Key Commands ####
    def append(self, key, value):
        return self.sharded(key, self.proxy.append, *[key, value])

    def decr(self, name, amount=1):
        return self.sharded(name, self.proxy.decr, *[name, amount])

    def exists(self, name):
        return self.shard_for_key(name).exists(name)

    def expire(self, name, time):
        return self.shard_for_key(name).expire(name, time)

    def expireat(self, name, time):
        return self.shard_for_key(name).expireat(name, time)

    def get(self, name):
        return self.shard_for_key(name).get(name)

    def getset(self, name, value):
        return self.sharded(name, self.proxy.getset, *[name, value])

    def incr(self, name, amount=1):
        if isinstance(amount, float):
            method = self.proxy.incrbyfloat
        else:
            method = self.proxy.incr
        return self.sharded(name, method, *[name, amount])

    def keys(self, pattern='*'):
        return sum([shard.keys(pattern) for shard in self.shards], [])

    def lock(self, name, **kwargs):
        return self.sharded(name, self.proxy.lock, *[name], **kwargs)

    def set(self, name, value, **kwargs):
        return self.sharded(name, self.proxy.set, *[name, value], **kwargs)

    def setex(self, name, value, time):
        return self.shard_for_key(name).setex(name, value, time)

    def setnx(self, name, value):
        return self.sharded(name, self.proxy.setnx, *[name, value])

    def substr(self, name, start, end=-1):
        return self.shard_for_key(name).substr(name, start, end)

    def ttl(self, name):
        return self.shard_for_key(name).ttl(name)

    def type(self, name):
        return self.shard_for_key(name).type(name)

    #### List Commands ####
    def lindex(self, name, index):
        return self.shard_for_key(name).lindex(name, index)

    def linsert(self, name, where, refvalue, value):
        return self.sharded(name, self.proxy.linsert,
                            *[name, where, refvalue, value])

    def llen(self, name):
        return self.shard_for_key(name).llen(name)

    def lpop(self, name):
        return self.sharded(name, self.proxy.lpop, *[name])

    def lpush(self, name, value):
        return self.sharded(name, self.proxy.lpush, *[name, value])

    def lpushx(self, name, value):
        return self.sharded(name, self.proxy.lpushx, *[name, value])

    def lrange(self, name, start, end):
        return self.shard_for_key(name).lrange(name, start, end)

    def lrem(self, name, value, num=0):
        return self.sharded(name, self.proxy.lrem, *[name, value, num])

    def lset(self, name, index, value):
        return self.sharded(name, self.proxy.lset, *[name, index, value])

    def ltrim(self, name, start, end):
        return self.sharded(name, self.proxy.ltrim, *[name, start, end])

    def rpop(self, name):
        return self.sharded(name, self.proxy.rpop, *[name])

    def rpush(self, name, value):
        return self.sharded(name, self.proxy.rpush, *[name, value])

    def rpushx(self, name, value):
        return self.sharded(name, self.proxy.rpushx, *[name, value])

    def sort(self, name, start=None, num=None, by=None,
             desc=False, alpha=False):
        return self.shard_for_key(name).sort(name, start=start, num=num, by=by,
                                             desc=desc, alpha=alpha)

    #### Set Commands ####
    def sadd(self, name, value):
        return self.sharded(name, self.proxy.sadd, *[name, value])

    def scard(self, name):
        return self.shard_for_key(name).scard(name)

    def sismember(self, name, value):
        return self.shard_for_key(name).sismember(name, value)

    def smembers(self, name):
        return self.shard_for_key(name).smembers(name)

    def spop(self, name):
        return self.sharded(name, self.proxy.spop, *[name])

    def srandmember(self, name):
        return self.shard_for_key(name).srandmember(name)

    def srem(self, name, key):
        return self.sharded(name, self.proxy.srem, *[name, key])

    #### Sorted Set Commands ####
    def zadd(self, name, value, score):
        return self.sharded(name, self.proxy.zadd, *[name, value, score])

    def zcard(self, name):
        return self.shard_for_key(name).zcard(name)

    def zincrby(self, name, value, amount=1):
        if isinstance(amount, float):
            method = self.proxy.zincrbyfloat
        else:
            method = self.proxy.zincr
        return self.sharded(name, method, *[name, value, amount])

    def zrange(self, name, start, end, desc=False, withscores=False):
        return self.shard_for_key(name).zrange(name, start, end,
                                               desc, withscores)

    def zrangebyscore(self, name, min, max,
                      start=None, num=None, withscores=False):
        return self.shard_for_key(name).zrangebyscore(name, min, max,
                                                      start, num, withscores)

    def zrank(self, name, value):
        return self.shard_for_key(name).zrank(name, value)

    def zrem(self, name, value):
        return self.sharded(name, self.proxy.zrem, *[name, value])

    def zremrangebyrank(self, name, min, max):
        return self.sharded(name, self.proxy.zremrangebyrank,
                            *[name, min, max])

    def zremrangebyscore(self, name, min, max):
        return self.sharded(name, self.proxy.zremrangebyscore,
                            *[name, min, max])

    def zrevrange(self, name, start, num, withscores=False):
        return self.shard_for_key(name).zrevrange(name, start, num, withscores)

    def zrevrank(self, name, value):
        return self.shard_for_key(name).zrevrank(name, value)

    def zscore(self, name, value):
        return self.shard_for_key(name).zscore(name, value)

    #### Hash Commands ####
    def hdel(self, name, key):
        return self.sharded(name, self.proxy.hdel, *[name, key])

    def hexists(self, name, key):
        return self.shard_for_key(name).hexists(name, key)

    def hget(self, name, key):
        return self.shard_for_key(name).hget(name, key)

    def hgetall(self, name):
        return self.shard_for_key(name).hgetall(name)

    def hincrby(self, name, key, amount=1):
        if isinstance(amount, float):
            method = self.proxy.hincrbyfloat
        else:
            method = self.proxy.hincrby
        return self.sharded(name, method, *[name, key, amount])

    def hkeys(self, name):
        return self.shard_for_key(name).hkeys(name)

    def hlen(self, name):
        return self.shard_for_key(name).hlen(name)

    def hset(self, name, key, value):
        return self.sharded(name, self.proxy.hset, *[name, key, value])

    def hsetnx(self, name, key, value):
        return self.sharded(name, self.proxy.hsetnx, *[name, key, value])

    def hmset(self, name, mapping):
        return self.sharded(name, self.proxy.hmset, *[name, mapping])

    def hmget(self, name, keys):
        return self.shard_for_key(name).hmget(name, keys)

    def hvals(self, name):
        return self.shard_for_key(name).hvals(name)

    # channels
    # Only implement subscribe/publish for single channels, no patterns
    def subscribe(self, channel):
        return self.shard_for_key(channel).subscribe([channel])

    def publish(self, channel, message):
        return self.shard_for_key(channel).publish(channel, message)

    def unsubscribe(self, channel):
        return self.shard_for_key(channel).unsubscribe([channel])


class ShardedRedis(ShardedProxy):
    def __init__(self, shards=[('localhost', 6379)], duration=None,
                 hashfn=None, db=0, password=None, socket_timeout=None,
                 charset='utf-8', errors='strict'):
        shards = [StrictRedis(host=x[0], port=x[1],
                              db=db, password=password,
                              socket_timeout=socket_timeout,
                              charset=charset, errors=errors)
                  for x in shards]
        super(ShardedRedis, self).__init__(shards, duration, hashfn)

    def sharded_pipeline(self, transaction=True):
        return ShardedPipeline([shard.pipeline() for shard in self.shards],
                               self.duration, self.hashfn)

    def with_keys_by_shard(self, shardfn, pattern='*'):
        """
        Gather all keys that match pattern, by shard,  then invoke shardfn
        once per shard, passing the shard, and the list of matching keys on
        that shard.
        """
        for shard in self.shards:
            keys = shard.keys(pattern)
            shardfn(shard, keys)


class ShardedPipeline(ShardedProxy):
    def __init__(self, shards, duration, hashfn):
        super(ShardedPipeline, self).__init__(shards, duration, hashfn)

    def execute(self):
        return [shard.execute() for shard in self.shards]
