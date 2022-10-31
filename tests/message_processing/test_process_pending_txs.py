import datetime as dt
from collections import defaultdict
from typing import Dict, List, Set

import pytest
import pytz
from aleph_message.models import Chain
from sqlalchemy import select

from aleph.db.models import PendingMessageDb, MessageStatusDb
from aleph.db.models.chains import ChainTxDb
from aleph.db.models.pending_txs import PendingTxDb
from aleph.jobs.process_pending_txs import PendingTxProcessor
from aleph.storage import StorageService
from aleph.types.actions.db_action import InsertPendingMessage, DeletePendingTx
from aleph.types.actions.db_executor import DbExecutor
from aleph.types.actions.executor import execute_actions
from aleph.types.chain_sync import ChainSyncProtocol
from aleph.types.db_session import DbSessionFactory
from aleph.types.message_status import MessageStatus
from .load_fixtures import load_fixture_messages


# TODO: try to replace this fixture by a get_json fixture. Currently, the pinning
#       of the message content gets in the way in the real get_chaindata_messages function.
async def get_fixture_chaindata_messages(
    pending_tx_content, pending_tx_context, seen_ids: List[str]
) -> List[Dict]:
    return load_fixture_messages(f"{pending_tx_content['content']}.json")


@pytest.mark.asyncio
async def test_process_pending_tx(
    mocker, session_factory: DbSessionFactory, test_storage_service: StorageService
):
    chain_data_service = mocker.AsyncMock()
    chain_data_service.get_chaindata_messages = get_fixture_chaindata_messages
    pending_tx_processor = PendingTxProcessor(
        session_factory=mocker.AsyncMock(), storage_service=test_storage_service
    )
    pending_tx_processor.chain_data_service = chain_data_service

    chain_tx = ChainTxDb(
        hash="0xf49cb176c1ce4f6eb7b9721303994b05074f8fadc37b5f41ac6f78bdf4b14b6c",
        chain=Chain.ETH,
        datetime=pytz.utc.localize(dt.datetime.utcfromtimestamp(1632835747)),
        height=13314512,
        publisher="0x23eC28598DCeB2f7082Cc3a9D670592DfEd6e0dC",
        protocol=ChainSyncProtocol.ON_CHAIN,
        protocol_version=1,
        content="test-data-pending-tx-messages",
    )

    pending_tx = PendingTxDb(tx=chain_tx)

    with session_factory() as session:
        session.add(pending_tx)
        session.commit()

    seen_ids: Set[str] = set()
    db_operations = await pending_tx_processor.handle_pending_tx(
        pending_tx=pending_tx, seen_ids=seen_ids
    )

    db_operations_by_type = defaultdict(list)
    for action in db_operations:
        db_operations_by_type[type(action)].append(action)

    assert set(db_operations_by_type.keys()) == {
        InsertPendingMessage,
        DeletePendingTx,
    }

    pending_tx_ops = db_operations_by_type[DeletePendingTx]
    assert len(pending_tx_ops) == 1

    pending_msg_ops = db_operations_by_type[InsertPendingMessage]
    fixture_messages = load_fixture_messages(f"{pending_tx.tx.content}.json")

    assert len(pending_msg_ops) == len(fixture_messages)

    await execute_actions(
        actions=db_operations, executors=DbExecutor(session_factory=session_factory)
    )
    with session_factory() as session:

        for fixture_message in fixture_messages:
            item_hash = fixture_message["item_hash"]
            message_status_db = (
                session.execute(
                    select(MessageStatusDb).where(
                        MessageStatusDb.item_hash == item_hash
                    )
                )
            ).scalar_one()
            assert message_status_db.status == MessageStatus.PENDING

            pending_message_db = (
                session.execute(
                    select(PendingMessageDb).where(
                        PendingMessageDb.item_hash == item_hash
                    )
                )
            ).scalar_one()

            # TODO: need utils to compare message DB types to dictionaries / Pydantic classes / etc
            assert pending_message_db.sender == fixture_message["sender"]
            assert pending_message_db.item_content == fixture_message["item_content"]
