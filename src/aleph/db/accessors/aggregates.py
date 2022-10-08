from typing import Optional, Iterable

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from aleph.db.models import AggregateDb, AggregateElementDb
from aleph.types.db_session import DbSession


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
