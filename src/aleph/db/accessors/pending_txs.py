from typing import AsyncIterator, Optional

from aleph_message.models import Chain
from sqlalchemy import select, func

from aleph.db.models import PendingTxDb, ChainTxDb
from aleph.types.db_session import DbSession


async def get_pending_txs_stream(session: DbSession) -> AsyncIterator[PendingTxDb]:
    select_stmt = (
        select(PendingTxDb)
        .join(ChainTxDb, PendingTxDb.tx_hash == ChainTxDb.hash)
        .order_by(ChainTxDb.datetime.asc())
    )
    return (await session.stream(select_stmt)).scalars()


async def count_pending_txs(session: DbSession, chain: Optional[Chain] = None) -> int:
    select_stmt = select(func.count(PendingTxDb.tx_hash))
    if chain:
        select_stmt = select_stmt.join(
            ChainTxDb, PendingTxDb.tx_hash == ChainTxDb.hash
        ).where(ChainTxDb.chain == chain)

    return (await session.execute(select_stmt)).scalar_one()
