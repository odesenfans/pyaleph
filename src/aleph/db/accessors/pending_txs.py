from typing import AsyncIterator, Optional

from aleph_message.models import Chain
from sqlalchemy import select, func

from aleph.db.models import PendingTxDb
from aleph.types.db_session import DbSession


async def get_pending_txs(session: DbSession) -> AsyncIterator[PendingTxDb]:
    select_stmt = select(PendingTxDb).order_by(PendingTxDb.time.asc())
    return await session.stream(select_stmt)


async def count_pending_txs(session: DbSession, chain: Optional[Chain] = None) -> int:
    select_stmt = select(func.count(PendingTxDb.tx_hash))
    if chain:
        select_stmt = select_stmt.where(PendingTxDb.chain == chain)

    return (await session.execute(select_stmt)).scalar_one()
