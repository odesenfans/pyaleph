import json
from pathlib import Path
from typing import Any, Dict, Sequence, cast

import pytest_asyncio
from aleph_message.models import AggregateContent, PostContent
from sqlalchemy import insert

from aleph.db.accessors.aggregates import refresh_aggregate
from aleph.db.models import (
    MessageDb,
    ChainTxDb,
    AggregateElementDb, message_confirmations,
)
from aleph.db.models.posts import PostDb
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.db_session import DbSessionFactory


# TODO: remove the raw parameter, it's just to avoid larger refactorings
async def _load_fixtures(
    session_factory: DbSessionFactory, filename: str, raw: bool = True
) -> Sequence[Dict[str, Any]]:
    fixtures_dir = Path(__file__).parent / "fixtures"
    fixtures_file = fixtures_dir / filename

    with fixtures_file.open() as f:
        messages_json = json.load(f)

    messages = []
    tx_hashes = set()

    with session_factory() as session:

        for message_dict in messages_json:
            message_db = MessageDb.from_message_dict(message_dict)
            messages.append(message_db)
            session.add(message_db)
            for confirmation in message_dict.get("confirmations", []):
                if (tx_hash := confirmation["hash"]) not in tx_hashes:
                    chain_tx_db = ChainTxDb.from_dict(confirmation)
                    tx_hashes.add(tx_hash)
                    session.add(chain_tx_db)

                session.flush()
                session.execute(
                    insert(message_confirmations).values(
                        item_hash=message_db.item_hash, tx_hash=tx_hash
                    )
                )
        session.commit()

    return messages_json if raw else messages


@pytest_asyncio.fixture
async def fixture_messages(
    session_factory: DbSessionFactory,
) -> Sequence[Dict[str, Any]]:
    return await _load_fixtures(session_factory, "fixture_messages.json")


def make_aggregate_element(message: MessageDb) -> AggregateElementDb:
    content = cast(AggregateContent, message.parsed_content)
    aggregate_element = AggregateElementDb(
        key=content.key,
        owner=content.address,
        content=content.content,
        item_hash=message.item_hash,
        creation_datetime=timestamp_to_datetime(content.time),
    )

    return aggregate_element


@pytest_asyncio.fixture
async def fixture_aggregate_messages(
    session_factory: DbSessionFactory,
) -> Sequence[MessageDb]:
    messages = await _load_fixtures(
        session_factory, "fixture_aggregates.json", raw=False
    )
    aggregate_keys = set()
    with session_factory() as session:
        for message in messages:
            aggregate_element = make_aggregate_element(message)  # type: ignore
            session.add(aggregate_element)
            aggregate_keys.add((aggregate_element.owner, aggregate_element.key))
        session.commit()

        for (owner, key) in aggregate_keys:
            refresh_aggregate(session=session, owner=owner, key=key)

        session.commit()

    return messages  # type: ignore


def make_post_db(message: MessageDb) -> PostDb:
    content = cast(PostContent, message.parsed_content)
    return PostDb(
        item_hash=message.item_hash,
        owner=content.address,
        type=content.type,
        ref=content.ref,
        amends=content.ref if content.type == "amend" else None,
        channel=message.channel,
        content=content.content,
        creation_datetime=timestamp_to_datetime(content.time),
    )


@pytest_asyncio.fixture
async def fixture_posts(
    session_factory: DbSessionFactory,
) -> Sequence[PostDb]:
    messages = await _load_fixtures(session_factory, "fixture_posts.json", raw=False)
    posts = [make_post_db(message) for message in messages]  # type: ignore

    with session_factory() as session:
        session.add_all(posts)
        session.commit()

    return posts
