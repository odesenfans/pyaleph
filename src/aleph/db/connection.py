from typing import Optional

from configmanager import Config
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from aleph.types.db_session import DbSessionFactory, AsyncDbSessionFactory

from aleph.config import get_config


def make_db_url(driver: str, config: Config) -> str:
    """
    Returns the database connection string from configuration values.

    :param config: Configuration. If not specified, the global configuration object is used.
    :returns: The database connection string.
    """

    host = config.postgres.host.value
    port = config.postgres.port.value
    user = config.postgres.user.value
    password = config.postgres.password.value
    database = config.postgres.database.value

    return f"postgresql+{driver}://{user}:{password}@{host}:{port}/{database}"


def make_engine(config: Optional[Config] = None, echo: bool = False) -> Engine:
    if config is None:
        config = get_config()

    return create_engine(
        make_db_url(driver="psycopg2", config=config),
        echo=False,
        pool_size=config.postgres.pool_size.value,
    )


def make_async_engine(
    config: Optional[Config] = None,
    echo: bool = False,
) -> AsyncEngine:
    return create_async_engine(
        make_db_url(driver="asyncpg", config=config), future=True, echo=echo
    )


def make_session_factory(engine: Engine) -> DbSessionFactory:
    return sessionmaker(engine, expire_on_commit=False)


def make_async_session_factory(engine: AsyncEngine) -> AsyncDbSessionFactory:
    return sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
