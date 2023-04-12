import logging
from typing import Annotated, Optional

from configmanager import Config
from fastapi import Depends
from sqlalchemy.engine import Engine

from aleph.config import get_config
from aleph.db.connection import make_engine, make_session_factory
from aleph.services.cache.node_cache import NodeCache
from aleph.types.db_session import DbSession


def app_config() -> Config:
    return get_config()


sqla_engine: Optional[Engine] = None


def db_engine(config: Annotated[Config, Depends(app_config)]) -> Engine:
    global sqla_engine

    if sqla_engine is None:
        sqla_engine = make_engine(
            config=config,
            echo=config.logging.level.value == logging.DEBUG,
            application_name="aleph-api",
        )

    return sqla_engine


def node_cache(config: Annotated[Config, Depends(app_config)]) -> NodeCache:
    print(f"{config.redis.host.value} - {config.redis.port.value}")

    return NodeCache(
        redis_host=config.redis.host.value, redis_port=config.redis.port.value
    )


async def db_session(engine: Annotated[Engine, Depends(db_engine)]) -> DbSession:
    session_factory = make_session_factory(engine)

    with session_factory() as session:
        yield session
        session.commit()
