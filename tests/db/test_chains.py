import datetime as dt

import pytest
import pytz
from aleph_message.models import Chain
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from aleph.db.accessors.chains import upsert_chain_sync_status, get_last_height
from aleph.db.models.chains import ChainSyncStatusDb


@pytest.mark.asyncio
async def test_get_last_height(session_factory: sessionmaker):
    eth_sync_status = ChainSyncStatusDb(
        chain=Chain.ETH,
        height=123,
        last_update=pytz.utc.localize(dt.datetime(2022, 10, 1)),
    )

    async with session_factory() as session:
        session.add(eth_sync_status)
        await session.commit()

    async with session_factory() as session:
        height = await get_last_height(session=session, chain=Chain.ETH)

    assert height == eth_sync_status.height


@pytest.mark.asyncio
async def test_get_last_height_no_data(session_factory: sessionmaker):
    async with session_factory() as session:
        height = await get_last_height(session=session, chain=Chain.NULS2)

    assert height is None


@pytest.mark.asyncio
async def test_upsert_chain_sync_status_insert(session_factory: sessionmaker):
    chain = Chain.ETH
    update_datetime = pytz.utc.localize(dt.datetime(2022, 11, 1))
    height = 10

    async with session_factory() as session:
        await upsert_chain_sync_status(
            session=session,
            chain=chain,
            height=height,
            update_datetime=update_datetime,
        )
        await session.commit()

    async with session_factory() as session:

        chain_sync_status = (
            await session.execute(
                select(ChainSyncStatusDb).where(ChainSyncStatusDb.chain == chain)
            )
        ).scalar()

    assert chain_sync_status.chain == chain
    assert chain_sync_status.height == height
    assert chain_sync_status.last_update == update_datetime


@pytest.mark.asyncio
async def test_upsert_peer_replace(session_factory: sessionmaker):
    existing_entry = ChainSyncStatusDb(
        chain=Chain.TEZOS,
        height=1000,
        last_update=pytz.utc.localize(dt.datetime(2023, 2, 6)),
    )

    async with session_factory() as session:
        session.add(existing_entry)
        await session.commit()

    new_height = 1001
    new_update_datetime = pytz.utc.localize(dt.datetime(2023, 2, 7))

    async with session_factory() as session:
        await upsert_chain_sync_status(
            session=session,
            chain=existing_entry.chain,
            height=new_height,
            update_datetime=new_update_datetime,
        )
        await session.commit()

    async with session_factory() as session:
        chain_sync_status = (
            await session.execute(
                select(ChainSyncStatusDb).where(
                    ChainSyncStatusDb.chain == existing_entry.chain
                )
            )
        ).scalar()

    assert chain_sync_status.chain == existing_entry.chain
    assert chain_sync_status.height == new_height
    assert chain_sync_status.last_update == new_update_datetime
