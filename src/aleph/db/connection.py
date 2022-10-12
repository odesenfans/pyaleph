from typing import Optional

from configmanager import Config
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from aleph.types.db_session import DbSessionFactory

from aleph.config import get_config


def get_db_url(config: Optional[Config] = None) -> str:
    """
    Returns the database connection string from configuration values.

    :param config: Configuration. If not specified, the global configuration object is used.
    :returns: The database connection string.
    """

    if config is None:
        config = get_config()

    host = config.postgres.host.value
    port = config.postgres.port.value
    user = config.postgres.user.value
    password = config.postgres.password.value
    database = config.postgres.database.value

    return f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{database}"


def make_engine(
    config: Optional[Config] = None,
    echo: bool = False,
) -> AsyncEngine:
    return create_async_engine(get_db_url(config=config), future=True, echo=echo)


def make_session_factory(engine: AsyncEngine) -> DbSessionFactory:
    return sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
