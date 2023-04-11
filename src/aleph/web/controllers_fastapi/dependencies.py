from typing import Annotated

from configmanager import Config
from fastapi import Depends

from aleph.services.cache.node_cache import NodeCache
from aleph.config import get_config


def config() -> Config:
    return get_config()


def node_cache(config: Annotated[Config, Depends(config)]) -> NodeCache:
    print(f"{config.redis.host.value} - {config.redis.port.value}")

    return NodeCache(
        redis_host=config.redis.host.value, redis_port=config.redis.port.value
    )
