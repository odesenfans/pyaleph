from typing import Optional, AsyncIterator

from aleph_message.models import Chain
from sqlalchemy import select, func
from sqlalchemy.orm import selectinload

from aleph.db.models import PendingMessageDb, ChainTxDb
from aleph.types.db_session import DbSession


async def get_pending_messages_stream(
    session: DbSession,
) -> AsyncIterator[PendingMessageDb]:
    select_stmt = (
        select(PendingMessageDb)
        .order_by(PendingMessageDb.retries.asc(), PendingMessageDb.time.asc())
        .options(selectinload(PendingMessageDb.tx))
    )
    return (await session.stream(select_stmt)).scalars()


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

    return (await session.execute(select_stmt)).scalar_one()
