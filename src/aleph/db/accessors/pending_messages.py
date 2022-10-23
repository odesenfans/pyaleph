from typing import Optional, Iterable

from aleph_message.models import Chain, MessageType
from sqlalchemy import select, func, update, delete
from sqlalchemy.orm import selectinload, aliased

from aleph.db.models import PendingMessageDb, ChainTxDb
from aleph.types.db_session import DbSession


async def get_pending_messages(
    session: DbSession,
    limit: int = 10000,
    offset: int = 0,
    skip_store: bool = False,
) -> Iterable[PendingMessageDb]:
    select_stmt = (
        select(PendingMessageDb)
        .order_by(PendingMessageDb.retries.asc(), PendingMessageDb.time.asc())
        .offset(offset)
        .options(selectinload(PendingMessageDb.tx))
    )
    if skip_store:
        subquery = aliased(PendingMessageDb, select_stmt.subquery())
        select_stmt = (
            select(subquery)
            .where(subquery.type != MessageType.store)
        )

    select_stmt = select_stmt.limit(limit)
    return (session.execute(select_stmt)).scalars()


async def count_pending_messages(
    session: DbSession, chain: Optional[Chain] = None
) -> int:
    """
    Counts pending messages.

    :param session: DB session.
    :param chain: If specified, the function will only count pending messages that were
                  confirmed on the specified chain.
    """
    select_stmt = select(func.count(PendingMessageDb.id))
    if chain:
        select_stmt = select_stmt.where(ChainTxDb.chain == chain).join(
            ChainTxDb, PendingMessageDb.tx_hash == ChainTxDb.hash
        )

    return (session.execute(select_stmt)).scalar_one()


async def increase_pending_message_retry_count(
    session: DbSession, pending_message: PendingMessageDb
):
    update_stmt = (
        update(PendingMessageDb)
        .where(PendingMessageDb.item_hash == pending_message.item_hash)
        .values(retries=PendingMessageDb.retries + 1)
    )
    session.execute(update_stmt)


async def delete_pending_message(session: DbSession, pending_message: PendingMessageDb):
    session.execute(
        delete(PendingMessageDb).where(PendingMessageDb.id == pending_message.id)
    )
