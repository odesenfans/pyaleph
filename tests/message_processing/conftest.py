import json
from pathlib import Path
from typing import Any, Dict, Sequence

import pytest
import pytest_asyncio

from aleph.db.models import ChainTxDb, PendingMessageDb
from aleph.types.db_session import DbSessionFactory
from .load_fixtures import load_fixture_messages


@pytest.fixture
def fixture_messages():
    return load_fixture_messages("test-data-pending-tx-messages.json")


# TODO: this code (and the fixture data) is duplicated with tests/api/conftest.py.
#       it could make sense to have some general fixtures available to all the test cases
#       to reduce duplication between DB tests, API tests, etc.
async def _load_fixtures(
    session_factory: DbSessionFactory, filename: str
) -> Sequence[Dict[str, Any]]:
    fixtures_dir = Path(__file__).parent / "fixtures"
    fixtures_file = fixtures_dir / filename

    with fixtures_file.open() as f:
        messages_json = json.load(f)

    pending_messages = []
    chain_txs = []
    tx_hashes = set()
    for message_dict in messages_json:
        pending_messages.append(PendingMessageDb.from_message_dict(message_dict))
        for confirmation in message_dict.get("confirmations", []):
            if (tx_hash := confirmation["hash"]) not in tx_hashes:
                chain_txs.append(ChainTxDb.from_dict(confirmation))
                tx_hashes.add(tx_hash)

    with session_factory() as session:
        session.add_all(pending_messages)
        session.add_all(chain_txs)
        session.commit()

    return messages_json


@pytest_asyncio.fixture
async def fixture_aggregate_messages(
    session_factory: DbSessionFactory,
) -> Sequence[Dict[str, Any]]:
    return await _load_fixtures(session_factory, "test-data-aggregates.json")
