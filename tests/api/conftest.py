import json
from pathlib import Path
from typing import Any, Dict, Sequence

import pytest_asyncio
from sqlalchemy.orm import sessionmaker

from aleph.db.models import MessageDb


async def _load_fixtures(
    session_factory: sessionmaker, filename: str
) -> Sequence[Dict[str, Any]]:
    fixtures_dir = Path(__file__).parent / "fixtures"
    fixtures_file = fixtures_dir / filename

    with fixtures_file.open() as f:
        messages_json = json.load(f)

    messages = [
        MessageDb.from_message_dict(message_dict=message_dict)
        for message_dict in messages_json
    ]

    async with session_factory() as session:
        session.add_all(messages)
        await session.commit()

    return messages_json


@pytest_asyncio.fixture
async def fixture_messages(session_factory: sessionmaker) -> Sequence[Dict[str, Any]]:
    return await _load_fixtures(session_factory, "fixture_messages.json")


@pytest_asyncio.fixture
async def fixture_aggregate_messages(
    session_factory: sessionmaker,
) -> Sequence[Dict[str, Any]]:
    return await _load_fixtures(session_factory, "fixture_aggregates.json")


@pytest_asyncio.fixture
async def fixture_post_messages(
    session_factory: sessionmaker,
) -> Sequence[Dict[str, Any]]:
    return await _load_fixtures(session_factory, "fixture_posts.json")
