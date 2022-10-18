import pytest
from aleph.types.db_session import DbSessionFactory

from aleph.db.accessors.files import is_pinned_file
from aleph.db.models import FilePinDb


@pytest.mark.asyncio
async def test_is_pinned_file(session_factory: DbSessionFactory):
    async def is_pinned(_session_factory, _file_hash) -> bool:
        with session_factory() as session:
            return await is_pinned_file(session=session, file_hash=_file_hash)

    file_hash = "QmTm7g1Mh3BhrQPjnedVQ5g67DR7cwhyMN3MvFt1JPPdWd"

    # We check for equality with True/False to determine that the function does indeed
    # return a boolean value
    assert await is_pinned(session_factory, file_hash) is False

    with session_factory() as session:
        session.add(FilePinDb(file_hash=file_hash, tx_hash="1234"))
        session.commit()

    assert await is_pinned(session_factory, file_hash) is True