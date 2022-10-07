import datetime as dt
from collections import defaultdict
from typing import Dict, List

import pytest
import pytz
from sqlalchemy.sql import Delete, Insert

from aleph.db.models import PendingMessageDb
from aleph.db.models.chains import ChainTxDb
from aleph.db.models.pending_txs import PendingTxDb, ChainSyncProtocol
from aleph.jobs.process_pending_txs import PendingTxProcessor
from aleph.storage import StorageService
from .load_fixtures import load_fixture_messages


# TODO: try to replace this fixture by a get_json fixture. Currently, the pinning
#       of the message content gets in the way in the real get_chaindata_messages function.
async def get_fixture_chaindata_messages(
    pending_tx_content, pending_tx_context, seen_ids: List[str]
) -> List[Dict]:
    return load_fixture_messages(f"{pending_tx_content['content']}.json")


@pytest.mark.asyncio
async def test_process_pending_tx(mocker, test_storage_service: StorageService):
    session_factory = mocker.AsyncMock()
    chain_data_service = mocker.AsyncMock()
    chain_data_service.get_chaindata_messages = get_fixture_chaindata_messages
    pending_tx_processor = PendingTxProcessor(
        session_factory=session_factory, storage_service=test_storage_service
    )
    pending_tx_processor.chain_data_service = chain_data_service

    chain_tx = ChainTxDb(
        hash="0xf49cb176c1ce4f6eb7b9721303994b05074f8fadc37b5f41ac6f78bdf4b14b6c",
        datetime=pytz.utc.localize(dt.datetime.utcfromtimestamp(1632835747)),
        height=13314512,
        publisher="0x23eC28598DCeB2f7082Cc3a9D670592DfEd6e0dC",
    )

    pending_tx = PendingTxDb(
        tx_hash=chain_tx.hash,
        protocol=ChainSyncProtocol.OffChain,
        protocol_version=1,
        content="test-data-pending-tx-messages",
        tx=chain_tx,
    )

    seen_ids: List[str] = []
    db_operations = await pending_tx_processor.handle_pending_tx(
        pending_tx=pending_tx, seen_ids=seen_ids
    )

    db_operations_by_model = defaultdict(list)
    for op in db_operations:
        db_operations_by_model[op.model].append(op)

    assert set(db_operations_by_model.keys()) == {PendingMessageDb, PendingTxDb}

    pending_tx_ops = db_operations_by_model[PendingTxDb]
    assert len(pending_tx_ops) == 1
    delete_sql_op = pending_tx_ops[0].operation
    assert isinstance(delete_sql_op, Delete)
    assert delete_sql_op.table == PendingTxDb.__table__

    pending_msg_ops = db_operations_by_model[PendingMessageDb]
    fixture_messages = load_fixture_messages(f"{pending_tx.content}.json")

    assert len(pending_msg_ops) == len(fixture_messages)
    # fixture_messages_by_hash = {msg["item_hash"]: msg for msg in fixture_messages}

    for pending_msg_op in pending_msg_ops:
        insert_sql_op = pending_msg_op.operation
        assert isinstance(insert_sql_op, Insert)
        assert insert_sql_op.table == PendingMessageDb.__table__
        # TODO: reactivate comparison tests. The easiest way might be to perform the insert
        #       statements and compare the expected values with the DB.
        assert False
        # pending_message = pending_msg_op.operation._doc["message"]
        # expected_message = fixture_messages_by_hash[
        #     pending_msg_op.operation._doc["message"]["item_hash"]
        # ]
        # # TODO: currently, the pending TX job modifies the time of the message.
        # del pending_message["time"]
        # assert set(pending_message.items()).issubset(set(expected_message.items()))
