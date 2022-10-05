import pytest
from sqlalchemy.orm import sessionmaker

from aleph.db.accessors.file_pins import is_pinned_file
from aleph.db.models import FilePinDb


@pytest.mark.asyncio
async def test_is_pinned_file(session_factory: sessionmaker):
    async def is_pinned(_session_factory, _file_hash) -> bool:
        async with session_factory() as session:
            return await is_pinned_file(session=session, file_hash=_file_hash)

    file_hash = "QmTm7g1Mh3BhrQPjnedVQ5g67DR7cwhyMN3MvFt1JPPdWd"

    # We check for equality with True/False to determine that the function does indeed
    # return a boolean value
    assert await is_pinned(session_factory, file_hash) is False

    async with session_factory() as session:
        session.add(FilePinDb(file_hash=file_hash, tx_hash="1234"))
        await session.commit()

    assert await is_pinned(session_factory, file_hash) is True
