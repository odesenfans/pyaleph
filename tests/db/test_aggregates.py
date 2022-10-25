import pytest
import sqlalchemy.orm.exc

from aleph.db.accessors.aggregates import get_aggregate_by_key
from aleph.db.models import AggregateDb, AggregateElementDb
from aleph.types.db_session import DbSessionFactory
import datetime as dt


@pytest.mark.asyncio
async def test_get_aggregate_by_key(session_factory: DbSessionFactory):
    key = "key"
    owner = "Me"
    creation_datetime = dt.datetime(2022, 1, 1)

    aggregate = AggregateDb(
        key=key,
        owner=owner,
        content={"a": 1, "b": 2},
        creation_datetime=creation_datetime,
        dirty=False,
        last_revision=AggregateElementDb(
            item_hash="1234",
            key=key,
            owner=owner,
            content={},
            creation_datetime=creation_datetime,
        ),
    )

    with session_factory() as session:
        session.add(aggregate)
        session.commit()

    with session_factory() as session:
        aggregate_db = await get_aggregate_by_key(session=session, owner=owner, key=key)
        assert aggregate_db
        assert aggregate_db.key == key
        assert aggregate_db.owner == owner
        assert aggregate_db.content == aggregate.content
        assert aggregate_db.last_revision_hash == aggregate.last_revision.item_hash

    # Try not loading the content
    with session_factory() as session:
        aggregate_db = await get_aggregate_by_key(
            session=session, owner=owner, key=key, with_content=False
        )

    assert aggregate_db
    with pytest.raises(sqlalchemy.orm.exc.DetachedInstanceError):
        _ = aggregate_db.content


@pytest.mark.asyncio
async def test_get_aggregate_by_key_no_data(session_factory: DbSessionFactory):
    with session_factory() as session:
        aggregate = await get_aggregate_by_key(
            session=session, owner="owner", key="key"
        )

    assert aggregate is None
