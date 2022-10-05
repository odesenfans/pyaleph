import datetime as dt
from typing import Optional

from aleph_message.models import Chain
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from ..models.chains import ChainSyncStatusDb


async def get_last_height(session: AsyncSession, chain: Chain) -> Optional[int]:
    height = (
        await session.execute(
            select(ChainSyncStatusDb.height).where(ChainSyncStatusDb.chain == chain)
        )
    ).scalar()
    return height


async def upsert_chain_sync_status(
    session: AsyncSession,
    chain: Chain,
    height: int,
    update_datetime: dt.datetime,
):
    upsert_stmt = (
        insert(ChainSyncStatusDb)
        .values(chain=chain, height=height, last_update=update_datetime)
        .on_conflict_do_update(
            constraint="chains_sync_status_pkey",
            set_={"height": height, "last_update": update_datetime},
        )
    )
    await session.execute(upsert_stmt)
