from typing import Optional, Iterable, List, Any, Dict, Tuple, Sequence

from aleph_message.models import ItemHash
from sqlalchemy import select, text, delete, update
from sqlalchemy.orm import selectinload

from aleph.db.models import AggregateDb, AggregateElementDb
from aleph.types.db_session import DbSession


async def aggregate_exists(session: DbSession, key: str, owner: str) -> bool:
    return await AggregateDb.exists(
        session=session,
        where=(AggregateDb.key == key) & (AggregateDb.owner == owner),
    )


async def get_aggregates_by_owner(
    session: DbSession, owner: str, keys: Optional[Sequence[str]] = None
) -> Iterable[Tuple[str, Dict[str, Any]]]:

    where_clause = AggregateDb.owner == owner
    if keys:
        where_clause = where_clause & AggregateDb.key.in_(keys)

    select_stmt = (
        select(AggregateDb.key, AggregateDb.content)
        .where(where_clause)
        .order_by(AggregateDb.key)
    )
    return session.execute(select_stmt).all()   # type: ignore


async def get_aggregate_by_key(
    session: DbSession, owner: str, key: str
) -> Optional[AggregateDb]:
    select_stmt = select(AggregateDb).where(
        (AggregateDb.owner == owner) & (AggregateDb.key == key)
    )
    return (
        session.execute(select_stmt.options(selectinload(AggregateDb.last_revision)))
    ).scalar()


async def get_aggregate_elements(
    session: DbSession, owner: str, key: str
) -> Iterable[AggregateElementDb]:
    select_stmt = (
        select(AggregateElementDb)
        .where((AggregateElementDb.owner == owner) & (AggregateElementDb.key == key))
        .order_by(AggregateElementDb.creation_datetime)
    )
    return (session.execute(select_stmt)).scalars()


async def get_message_hashes_for_aggregate(
    session: DbSession, owner: str, key: str
) -> Iterable[ItemHash]:
    select_stmt = select(AggregateElementDb.item_hash).where(
        (AggregateElementDb.key == key) & (AggregateElementDb.owner == owner)
    )
    return (ItemHash(result) for result in (session.execute(select_stmt)).scalars())


async def delete_aggregate(session: DbSession, owner: str, key: str):
    delete_aggregate_stmt = delete(AggregateDb).where(
        (AggregateDb.key == key) & (AggregateDb.owner == owner)
    )
    delete_elements_stmt = delete(AggregateElementDb).where(
        (AggregateDb.key == key) & (AggregateDb.owner == owner)
    )

    session.execute(delete_aggregate_stmt)
    session.execute(delete_elements_stmt)


def merge_aggregate_elements(elements: Iterable[AggregateElementDb]) -> Dict:
    content = {}
    for element in elements:
        content.update(element.content)
    return content


async def refresh_aggregate(session: DbSession, owner: str, key: str):
    elements = list(
        session.execute(
            select(AggregateElementDb)
            .where(
                (AggregateElementDb.key == key) & (AggregateElementDb.owner == owner)
            )
            .order_by(AggregateElementDb.creation_datetime)
        ).scalars()
    )
    content = merge_aggregate_elements(elements)

    session.execute(
        update(AggregateDb)
        .where((AggregateElementDb.key == key) & (AggregateElementDb.owner == owner))
        .values(
            content=content,
            creation_datetime=elements[0].creation_datetime,
            last_revision_hash=elements[-1].item_hash,
        )
    )


async def delete_aggregate_element(session: DbSession, item_hash: str):
    delete_element_stmt = delete(AggregateElementDb).where(
        AggregateElementDb.item_hash == item_hash
    )
    session.execute(delete_element_stmt)
