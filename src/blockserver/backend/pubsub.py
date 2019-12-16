from __future__ import annotations
import aioredis
from tornado import ioloop

from .. import monitoring


class AbstractSubscribe:
    """
    Subscription interface, part of pubsub.

    First call subscribe(channel), then iterate over this object. When done, close() it.
    """

    async def subscribe(self, channel, wildcard=False):
        """
        Subscribe to *channel*. If *wildcards* are used, then glob-style patterns (* expansion) must be applied.

        An instance may only be subscribed to one channel. The latest subscription wins.
        """

    def __aiter__(self):
        """
        Return async iterator yielding incoming messages as dictionaries.
        """

    async def close(self):
        """
        Close connection.
        """


class AsyncRedisSubscribe(AbstractSubscribe):
    def __init__(self, connection_pool: aioredis.ConnectionsPool):
        self.connection_pool = connection_pool
        self.channel = None
        self.connection = None

    async def subscribe(self, channel, wildcard=False):
        self.connection = await self.connection_pool.acquire()
        monitoring.PUBSUB_OPEN_CONNECTIONS.inc()
        redis_channel = aioredis.Channel(channel, is_pattern=wildcard)
        if wildcard:
            command = 'psubscribe'
        else:
            command = 'subscribe'
        await self.connection.execute_pubsub(command, redis_channel)
        self.channel = redis_channel

    async def close(self):
        await self.connection.unsubscribe()
        self.channel.close()
        self.connection_pool.release(self.connection)
        self.connection = None
        monitoring.PUBSUB_OPEN_CONNECTIONS.dec()

    def __aiter__(self):
        # See also: PEP-0525
        if not self.channel:
            raise ValueError('Not subscribed, cannot iterator over messages.')
        # Note: can't use the line below instead, because of the pattern<->non-pattern distinction of channel.get()
        # self.channel.iter(encoding='utf-8', decoder=json.loads)
        return self

    async def __anext__(self):
        message = await self.channel.get_json()
        if self.channel.is_pattern:
            return message[1]
        else:
            return message


async def redis_publish(connection_pool: aioredis.Redis, channel, message):
    import json
    with await connection_pool as connection:
        as_json = json.dumps(message)
        await connection.execute('publish', channel, as_json)
    monitoring.PUBSUB_PUBLISHED.inc()
