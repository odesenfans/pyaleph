from typing import Any, Set, Optional

import redis.asyncio as redis_asyncio

CacheKey = Any
CacheValue = bytes


class NodeCache:
    API_SERVERS_KEY = "api_servers"
    redis_client: redis_asyncio.Redis

    def __init__(self, redis_host: str, redis_port: int):
        self.redis_host = redis_host
        self.redis_port = redis_port

        self.redis_client = redis_asyncio.Redis(host=redis_host, port=redis_port)

    async def get(self, key: CacheKey) -> Optional[CacheValue]:
        return await self.redis_client.get(key)

    async def set(self, key: CacheKey, value: Any):
        await self.redis_client.set(key, value)

    async def incr(self, key: CacheKey):
        await self.redis_client.incr(key)

    async def decr(self, key: CacheKey):
        await self.redis_client.decr(key)

    async def get_api_servers(self) -> Set[str]:
        return set(
            api_server.decode()
            for api_server in await self.redis_client.smembers(self.API_SERVERS_KEY)
        )

    async def add_api_server(self, api_server: str) -> None:
        await self.redis_client.sadd(self.API_SERVERS_KEY, api_server)

    async def has_api_server(self, api_server: str) -> bool:
        return await self.redis_client.sismember(self.API_SERVERS_KEY, api_server)

    async def remove_api_server(self, api_server: str) -> None:
        await self.redis_client.srem(self.API_SERVERS_KEY, api_server)
