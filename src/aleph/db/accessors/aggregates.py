from typing import Optional, Iterable, List, Any, Dict, Tuple, Sequence

from aleph_message.models import ItemHash
from sqlalchemy import select, text, delete, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import selectinload

from aleph.db.models import AggregateDb, AggregateElementDb
from aleph.types.db_session import DbSession
import datetime as dt


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
    return session.execute(select_stmt).all()  # type: ignore


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


async def insert_aggregate(
    session: DbSession,
    key: str,
    owner: str,
    content: Dict[str, Any],
    creation_datetime: dt.datetime,
    last_revision_hash: str,
):
    insert_stmt = insert(AggregateDb).values(
        key=key,
        owner=owner,
        content=content,
        creation_datetime=creation_datetime,
        last_revision_hash=last_revision_hash,
    )
    session.execute(insert_stmt)


async def update_aggregate(
    session: DbSession,
    key: str,
    owner: str,
    content: Dict[str, Any],
    creation_datetime: dt.datetime,
    last_revision_hash: str,
):
    update_stmt = (
        update(AggregateDb)
        .values(
            content=content,
            creation_datetime=creation_datetime,
            last_revision_hash=last_revision_hash,
        )
        .where((AggregateDb.key == key) & (AggregateDb.owner == owner))
    )
    session.execute(update_stmt)


async def insert_aggregate_element(
    session: DbSession,
    item_hash: str,
    key: str,
    owner: str,
    content: Dict[str, Any],
    creation_datetime: dt.datetime,
):
    insert_stmt = insert(AggregateElementDb).values(
        item_hash=item_hash,
        key=key,
        owner=owner,
        content=content,
        creation_datetime=creation_datetime,
    )
    session.execute(insert_stmt)


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
    try:
        for element in elements:
            content.update(element.content)
    except ValueError:
        print("merge:", elements)
        print([el.item_hash for el in elements])
        print([el.content for el in elements])
        raise
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

    insert_stmt = insert(AggregateDb).values(
        key=key,
        owner=owner,
        content=content,
        creation_datetime=elements[0].creation_datetime,
        last_revision_hash=elements[-1].item_hash,
    )
    upsert_aggregate_stmt = insert_stmt.on_conflict_do_update(
        constraint="aggregates_pkey",
        set_={
            "content": insert_stmt.excluded.content,
            "creation_datetime": insert_stmt.excluded.creation_datetime,
            "last_revision_hash": insert_stmt.excluded.last_revision_hash,
        },
    )

    session.execute(upsert_aggregate_stmt)


async def delete_aggregate_element(session: DbSession, item_hash: str):
    delete_element_stmt = delete(AggregateElementDb).where(
        AggregateElementDb.item_hash == item_hash
    )
    session.execute(delete_element_stmt)
