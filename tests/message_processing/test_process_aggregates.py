import json
from typing import Dict, List

import pytest
from configmanager import Config
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from aleph.db.accessors.aggregates import get_aggregate_by_key, get_aggregate_elements
from aleph.db.models import PendingMessageDb
from aleph.handlers.message_handler import MessageHandler
from aleph.jobs.process_pending_messages import PendingMessageProcessor
from aleph.storage import StorageService
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.db_session import DbSessionFactory


@pytest.mark.asyncio
async def test_process_aggregate_first_element(
    mocker, session_factory: DbSessionFactory, fixture_aggregate_messages: List[Dict]
):
    storage_service = StorageService(
        storage_engine=mocker.AsyncMock(), ipfs_service=mocker.AsyncMock()
    )
    chain_service = mocker.AsyncMock()
    message_handler = MessageHandler(
        session_factory=session_factory,
        chain_service=chain_service,
        storage_service=storage_service,
    )

    item_hash = "a87004aa03f8ae63d2c4bbe84b93b9ce70ca6482ce36c82ab0b0f689fc273f34"

    with session_factory() as session:
        pending_message = (
            session.execute(
                select(PendingMessageDb)
                .where(PendingMessageDb.item_hash == item_hash)
                .options(selectinload(PendingMessageDb.tx))
            )
        ).scalar_one()

    await message_handler.fetch_and_process_one_message_db(
        pending_message=pending_message
    )

    # Check the aggregate
    content = json.loads(pending_message.item_content)

    expected_key = content["key"]
    expected_creation_datetime = timestamp_to_datetime(content["time"])

    with session_factory() as session:
        elements = list(
            await get_aggregate_elements(
                session=session, key=expected_key, owner=pending_message.sender
            )
        )
        assert len(elements) == 1
        element = elements[0]
        assert element.key == expected_key
        assert element.creation_datetime == expected_creation_datetime
        assert element.content == content["content"]

        aggregate = await get_aggregate_by_key(
            session=session,
            owner=pending_message.sender,
            key=expected_key,
        )

        assert aggregate
        assert aggregate.key == expected_key
        assert aggregate.owner == pending_message.sender
        assert aggregate.content == content["content"]
        assert aggregate.creation_datetime == expected_creation_datetime
        assert aggregate.last_revision_hash == element.item_hash


@pytest.mark.asyncio
async def test_process_aggregates(
    session_factory: DbSessionFactory,
    mock_config: Config,
    message_processor: PendingMessageProcessor,
    fixture_aggregate_messages: List[Dict],
):
    with session_factory() as session:
        pipeline = message_processor.make_pipeline(
            session=session,
            config=mock_config,
            shared_stats={"message_jobs": {}},
            loop=False,
        )
        messages = [message async for message in pipeline]
        print(messages)

    # TODO: improve this test
