import datetime as dt
from typing import Dict

import pytest
import pytz
from aleph_message.models import Chain
from sqlalchemy import delete, insert, select
from sqlalchemy.sql import Insert

from aleph.db.bulk_operations import DbBulkOperation
from aleph.db.models import PendingTxDb, PendingMessageDb
from aleph.db.models.chains import ChainTxDb
from aleph.jobs.job_utils import perform_db_operations
from aleph.types.chain_sync import ChainSyncProtocol
from aleph.types.db_session import DbSessionFactory

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
        protocol=ChainSyncProtocol.OFF_CHAIN,
        protocol_version=1,
        content="tx-content",
    )


@pytest.fixture
def pending_tx(chain_tx):
    return PendingTxDb(tx=chain_tx)


async def insert_chain_tx(session_factory: DbSessionFactory, chain_tx: ChainTxDb):
    with session_factory() as session:
        session.add(chain_tx)
        session.commit()


@pytest.mark.asyncio
async def test_db_operations_insert_one(session_factory, chain_tx, pending_tx):
    await insert_chain_tx(session_factory, chain_tx)

    db_operations = [
        DbBulkOperation(
            model=PendingTxDb,
            operation=insert(PendingTxDb).values(
                tx_hash=chain_tx.hash,
            ),
        )
    ]

    with session_factory() as session:
        start_count = await PendingTxDb.count(session)
        await perform_db_operations(session, db_operations)
        session.commit()

        end_count = await PendingTxDb.count(session)
        stored_pending_tx = (
            session.execute(
                select(PendingTxDb).where(PendingTxDb.tx_hash == chain_tx.hash)
            )
        ).scalar()

        assert stored_pending_tx.tx.content == pending_tx.tx.content
        # assert stored_pending_tx["context"] == PENDING_TX["context"]
        assert end_count - start_count == 1


@pytest.mark.asyncio
async def test_db_operations_delete_one(
    session_factory: DbSessionFactory, chain_tx: ChainTxDb, pending_tx: PendingTxDb
):

    with session_factory() as session:
        session.add(chain_tx)
        session.add(pending_tx)
        session.commit()

        start_count = await PendingTxDb.count(session)

    db_operations = [
        DbBulkOperation(
            model=PendingTxDb,
            operation=delete(PendingTxDb).where(
                PendingTxDb.tx_hash == pending_tx.tx_hash
            ),
        )
    ]

    with session_factory() as session:
        await perform_db_operations(session, db_operations)
        session.commit()

        end_count = await PendingTxDb.count(session)

    assert end_count - start_count == -1


def make_insert_message_statement(msg: Dict) -> Insert:
    values = msg.copy()
    values["time"] = pytz.utc.localize(dt.datetime.utcfromtimestamp(msg["time"]))

    return insert(PendingMessageDb).values(
        **values,
        retries=0,
        check_message=True,
        reception_time=values["time"] + dt.timedelta(seconds=1)
    )


@pytest.mark.asyncio
async def test_db_operations_insert_and_delete(
    session_factory: DbSessionFactory,
    fixture_messages,
    chain_tx: ChainTxDb,
    pending_tx: PendingTxDb,
):
    """
    Test a typical case where we insert several messages and delete a pending TX.
    """

    with session_factory() as session:
        session.add(chain_tx)
        session.add(pending_tx)
        session.commit()

        tx_start_count = await PendingTxDb.count(session)
        msg_start_count = await PendingMessageDb.count(session)

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

    with session_factory() as session:
        await perform_db_operations(session, db_operations)
        session.commit()

        tx_end_count = await PendingTxDb.count(session)
        msg_end_count = await PendingMessageDb.count(session)

        messages_db = session.execute(
            select(PendingMessageDb).where(
                PendingMessageDb.item_hash.in_(
                    [msg["item_hash"] for msg in fixture_messages]
                )
            )
        ).scalars()

        assert tx_end_count - tx_start_count == -1
        assert msg_end_count - msg_start_count == len(fixture_messages)

        # Check each message
        fixture_messages_by_hash = {msg["item_hash"]: msg for msg in fixture_messages}
        for pending_message in messages_db:
            expected_message = fixture_messages_by_hash[pending_message.item_hash]
            assert pending_message.item_hash == expected_message["item_hash"]
            assert pending_message.type == expected_message["type"]
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
