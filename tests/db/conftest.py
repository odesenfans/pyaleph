import pytest_asyncio

from aleph.db.connection import make_engine, make_session_factory
from aleph.db.models.base import Base


@pytest_asyncio.fixture
async def session_factory(mock_config):
    engine = make_engine(mock_config, echo=True)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    return make_session_factory(engine)
