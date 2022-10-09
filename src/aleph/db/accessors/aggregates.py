from typing import Optional, Iterable, List

from aleph_message.models import ItemHash
from sqlalchemy import select, text, delete
from sqlalchemy.orm import selectinload

from aleph.db.models import AggregateDb, AggregateElementDb
from aleph.types.db_session import DbSession


async def aggregate_exists(session: DbSession, key: str, owner: str) -> bool:
    return await AggregateDb.exists(
        session=session,
        where=(AggregateDb.key == key) & (AggregateDb.owner == owner),
    )


async def get_aggregate_by_key(
    session: DbSession, owner: str, key: str
) -> Optional[AggregateDb]:
    select_stmt = select(AggregateDb).where(
        (AggregateDb.owner == owner) & (AggregateDb.key == key)
    )
    return (
        await session.execute(
            select_stmt.options(selectinload(AggregateDb.last_revision))
        )
    ).scalar()


async def get_aggregate_elements(
    session: DbSession, owner: str, key: str
) -> Iterable[AggregateElementDb]:
    select_stmt = (
        select(AggregateElementDb)
        .where((AggregateElementDb.owner == owner) & (AggregateElementDb.key == key))
        .order_by(AggregateElementDb.creation_datetime)
    )
    return (await session.execute(select_stmt)).scalars()


async def get_message_hashes_for_aggregate(
    session: DbSession, owner: str, key: str
) -> Iterable[ItemHash]:
    select_stmt = select(AggregateElementDb.item_hash).where(
        (AggregateElementDb.key == key) & (AggregateElementDb.owner == owner)
    )
    return (ItemHash(result) for result in (await session.execute(select_stmt)).scalars())


async def delete_aggregate(session: DbSession, owner: str, key: str):
    delete_aggregate_stmt = delete(AggregateDb).where(
        (AggregateDb.key == key) & (AggregateDb.owner == owner)
    )
    delete_elements_stmt = delete(AggregateElementDb).where(
        (AggregateDb.key == key) & (AggregateDb.owner == owner)
    )

    await session.execute(delete_aggregate_stmt)
    await session.execute(delete_elements_stmt)
