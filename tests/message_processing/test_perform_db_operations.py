from typing import Dict

import pytest
import pytz
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import Insert

from aleph.db.models import PendingTxDb, ChainSyncProtocol, PendingMessageDb
from aleph.jobs.job_utils import perform_db_operations
from aleph.db.bulk_operations import DbBulkOperation
from sqlalchemy import delete, func, insert, select, Column

import datetime as dt
from aleph_message.models import Chain
from aleph.db.models.chains import ChainTxDb

PENDING_TX = {
    "content": {
        "protocol": "aleph-offchain",
        "version": 1,
        "content": "test-data-pending-tx-messages",
    },
    "context": {
        "chain_name": "ETH",
        "tx_hash": "0xf49cb176c1ce4f6eb7b9721303994b05074f8fadc37b5f41ac6f78bdf4b14b6c",
        "time": 1632835747,
        "height": 13314512,
        "publisher": "0x23eC28598DCeB2f7082Cc3a9D670592DfEd6e0dC",
    },
}

CHAIN_TX = ChainTxDb(
    chain=Chain.ETH,
    hash="0xf49cb176c1ce4f6eb7b9721303994b05074f8fadc37b5f41ac6f78bdf4b14b6c",
    datetime=dt.datetime.utcfromtimestamp(1632835747),
    height=13314512,
    publisher="0x23eC28598DCeB2f7082Cc3a9D670592DfEd6e0dC",
)


@pytest.fixture
def chain_tx():
    return ChainTxDb(
        chain=Chain.ETH,
        hash="0xf49cb176c1ce4f6eb7b9721303994b05074f8fadc37b5f41ac6f78bdf4b14b6c",
        datetime=dt.datetime.utcfromtimestamp(1632835747),
        height=13314512,
        publisher="0x23eC28598DCeB2f7082Cc3a9D670592DfEd6e0dC",
    )


@pytest.fixture
def pending_tx(chain_tx):
    return PendingTxDb(
        tx_hash=chain_tx.hash,
        protocol=ChainSyncProtocol.OffChain,
        protocol_version=1,
        content="test-data-pending-tx-messages",
    )


async def insert_chain_tx(session_factory: sessionmaker, chain_tx: ChainTxDb):
    async with session_factory() as session:
        session.add(chain_tx)
        await session.commit()


async def count_rows(session: AsyncSession, column: Column):
    return (await session.execute(func.count(column))).scalar_one()


async def count_pending_txs(session: AsyncSession):
    return await count_rows(session, PendingTxDb.tx_hash)


async def count_pending_messages(session: AsyncSession):
    return await count_rows(session, PendingMessageDb.item_hash)


@pytest.mark.asyncio
async def test_db_operations_insert_one(session_factory, chain_tx, pending_tx):
    await insert_chain_tx(session_factory, chain_tx)

    db_operations = [
        DbBulkOperation(
            model=PendingTxDb,
            operation=insert(PendingTxDb).values(
                tx_hash=chain_tx.hash,
                protocol=pending_tx.protocol,
                protocol_version=pending_tx.protocol_version,
                content=pending_tx.content,
            ),
        )
    ]

    async with session_factory() as session:
        start_count = await count_pending_txs(session)
        await perform_db_operations(session, db_operations)
        await session.commit()

        end_count = await count_pending_txs(session)
        stored_pending_tx = (
            await session.execute(
                select(PendingTxDb).where(PendingTxDb.tx_hash == chain_tx.hash)
            )
        ).scalar()

    assert stored_pending_tx.content == pending_tx.content
    # assert stored_pending_tx["context"] == PENDING_TX["context"]
    assert end_count - start_count == 1


@pytest.mark.asyncio
async def test_db_operations_delete_one(
    session_factory: sessionmaker, chain_tx: ChainTxDb, pending_tx: PendingTxDb
):

    async with session_factory() as session:
        session.add(chain_tx)
        session.add(pending_tx)
        await session.commit()

        start_count = await count_pending_txs(session)

    db_operations = [
        DbBulkOperation(
            model=PendingTxDb,
            operation=delete(PendingTxDb).where(
                PendingTxDb.tx_hash == pending_tx.tx_hash
            ),
        )
    ]

    async with session_factory() as session:
        await perform_db_operations(session, db_operations)
        await session.commit()

        end_count = await count_pending_txs(session)

    assert end_count - start_count == -1


def make_insert_message_statement(msg: Dict) -> Insert:
    values = msg.copy()
    values["time"] = pytz.utc.localize(dt.datetime.utcfromtimestamp(msg["time"]))
    values["message_type"] = msg["type"]
    del values["type"]

    return insert(PendingMessageDb).values(**values, retries=0, check_message=True)


@pytest.mark.asyncio
async def test_db_operations_insert_and_delete(
    session_factory: sessionmaker,
    fixture_messages,
    chain_tx: ChainTxDb,
    pending_tx: PendingTxDb,
):
    """
    Test a typical case where we insert several messages and delete a pending TX.
    """

    async with session_factory() as session:
        session.add(chain_tx)
        session.add(pending_tx)
        await session.commit()

        tx_start_count = await count_pending_txs(session)
        msg_start_count = await count_pending_messages(session)

    db_operations = [
        DbBulkOperation(
            model=PendingMessageDb, operation=make_insert_message_statement(msg)
        )
        for msg in fixture_messages
    ]

    db_operations.append(
        DbBulkOperation(
            model=PendingTxDb,
            operation=delete(PendingTxDb).where(
                PendingTxDb.tx_hash == pending_tx.tx_hash
            ),
        )
    )

    async with session_factory() as session:
        await perform_db_operations(session, db_operations)
        await session.commit()

        tx_end_count = await count_pending_txs(session)
        msg_end_count = await count_pending_messages(session)

        messages_db = await session.execute(
            select(PendingMessageDb).where(
                PendingMessageDb.item_hash.in_(
                    [msg["item_hash"] for msg in fixture_messages]
                )
            )
        )

    assert tx_end_count - tx_start_count == -1
    assert msg_end_count - msg_start_count == len(fixture_messages)

    # Check each message
    fixture_messages_by_hash = {msg["item_hash"]: msg for msg in fixture_messages}
    for pending_message in messages_db.scalars():
        expected_message = fixture_messages_by_hash[pending_message.item_hash]
        assert pending_message.item_hash == expected_message["item_hash"]
        assert pending_message.message_type == expected_message["type"]
        assert pending_message.chain == expected_message["chain"]
        assert pending_message.sender == expected_message["sender"]
        assert pending_message.signature == expected_message["signature"]
        assert pending_message.item_type == expected_message["item_type"]
        assert pending_message.item_content == expected_message["item_content"]
        assert pending_message.channel == expected_message["channel"]
        assert pending_message.time.timestamp() == expected_message["time"]

        assert pending_message.retries == 0
        assert pending_message.check_message
        assert pending_message.tx_hash is None
