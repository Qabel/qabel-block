import json

import aioredis
from tornado import ioloop

from .. import monitoring


class AbstractPublishSubscribe:
    async def subscribe(self, channel, wildcard=False):
        """
        Subscribe to *channel*. If *wildcards* are used, then glob-style patterns (* expansion) must be applied.

        An instance may only be subscribed to one channel. The latest subscription wins.
        """

    async def publish(self, channel, message):
        """
        Publish *message* dictionary to *channel*.
        """

    def __aiter__(self):
        """
        Return async iterator yielding incoming messages as dictionaries.
        """

    async def close(self):
        """
        Close connection.
        """


class AsyncRedisPublishSubscribe(AbstractPublishSubscribe):
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self._redis = None
        self.channel = None

    async def redis(self):
        if not self._redis:
            loop = ioloop.IOLoop.current().asyncio_loop
            self._redis = await aioredis.create_redis((self.host, self.port), loop=loop)
            monitoring.PUBSUB_OPEN_CONNECTIONS.inc()
        return self._redis

    async def close(self):
        (await self.redis()).close()
        monitoring.PUBSUB_OPEN_CONNECTIONS.dec()

    async def subscribe(self, channel, wildcard=False):
        if wildcard:
            subscriber = (await self.redis()).psubscribe
        else:
            subscriber = (await self.redis()).subscribe
        self.channel, = await subscriber(channel)

    async def publish(self, channel, message):
        await (await self.redis()).publish_json(channel, message)
        monitoring.PUBSUB_PUBLISHED.inc()

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
