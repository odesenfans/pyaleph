import json
from pathlib import Path
from typing import Any, Dict, Sequence

import pytest_asyncio

from aleph.db.models import MessageDb, ChainTxDb, MessageConfirmationDb
from aleph.types.db_session import DbSessionFactory


async def _load_fixtures(
    session_factory: DbSessionFactory, filename: str
) -> Sequence[Dict[str, Any]]:
    fixtures_dir = Path(__file__).parent / "fixtures"
    fixtures_file = fixtures_dir / filename

    with fixtures_file.open() as f:
        messages_json = json.load(f)

    messages = []
    confirmations = []
    chain_txs = []
    tx_hashes = set()
    for message_dict in messages_json:
        messages.append(MessageDb.from_message_dict(message_dict))
        for confirmation in message_dict.get("confirmations", []):
            if (tx_hash := confirmation["hash"]) not in tx_hashes:
                chain_txs.append(ChainTxDb.from_dict(confirmation))
                tx_hashes.add(tx_hash)

            confirmations.append(
                MessageConfirmationDb(
                    item_hash=message_dict["item_hash"], tx_hash=tx_hash
                )
            )

    with session_factory() as session:
        session.add_all(messages)
        session.add_all(chain_txs)
        session.add_all(confirmations)
        session.commit()

    return messages_json


@pytest_asyncio.fixture
async def fixture_messages(session_factory: DbSessionFactory) -> Sequence[Dict[str, Any]]:
    return await _load_fixtures(session_factory, "fixture_messages.json")


@pytest_asyncio.fixture
async def fixture_aggregate_messages(
    session_factory: DbSessionFactory,
) -> Sequence[Dict[str, Any]]:
    return await _load_fixtures(session_factory, "fixture_aggregates.json")


@pytest_asyncio.fixture
async def fixture_post_messages(
    session_factory: DbSessionFactory,
) -> Sequence[Dict[str, Any]]:
    return await _load_fixtures(session_factory, "fixture_posts.json")
