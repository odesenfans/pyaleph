import datetime as dt

import pytest
import pytz
from aleph_message.models import Chain, MessageType, ItemType

from aleph.db.accessors.messages import get_message_by_item_hash
from aleph.db.models import MessageDb, MessageConfirmationDb, ChainTxDb
from aleph.types.db_session import DbSessionFactory


@pytest.fixture
def fixture_message() -> MessageDb:
    # TODO: use a valid message, this one has incorrect signature, size, etc.

    sender = "0x51A58800b26AA1451aaA803d1746687cB88E0500"
    return MessageDb(
        item_hash="aea68aac5f4dc6e6b813fc5de9e6c17d3ef1b03e77eace15398405260baf3ce4",
        chain=Chain.ETH,
        sender=sender,
        signature="0x705ca1365a0b794cbfcf89ce13239376d0aab0674d8e7f39965590a46e5206a664bc4b313f3351f313564e033c9fe44fd258492dfbd6c36b089677d73224da0a1c",
        message_type=MessageType.aggregate,
        content={
            "address": sender,
            "key": "my-aggregate",
            "time": 1664999873,
            "content": {"easy": "as", "a-b": "c"},
        },
        item_type=ItemType.inline,
        size=2000,
        time=pytz.utc.localize(dt.datetime.utcfromtimestamp(1664999872)),
        channel="CHANEL-N5",
    )


def compare_messages(expected: MessageDb, actual: MessageDb):
    assert actual.item_hash == expected.item_hash
    assert actual.chain == expected.chain
    assert actual.sender == expected.sender
    assert actual.signature == expected.signature
    assert actual.message_type == expected.message_type
    assert actual.content == expected.content
    assert actual.item_type == expected.item_type
    assert actual.size == expected.size
    assert actual.time == expected.time
    assert actual.channel == expected.channel


@pytest.mark.asyncio
async def test_get_message(session_factory: DbSessionFactory, fixture_message: MessageDb):
    async with session_factory() as session:
        session.add(fixture_message)
        await session.commit()

    async with session_factory() as session:
        fetched_message = await get_message_by_item_hash(
            session=session, item_hash=fixture_message.item_hash
        )

    assert fetched_message is not None
    compare_messages(expected=fixture_message, actual=fetched_message)

    # Check confirmation fields/properties
    assert fetched_message.confirmations == []
    assert not fetched_message.confirmed


@pytest.mark.asyncio
async def test_get_message_with_confirmations(
    session_factory: DbSessionFactory, fixture_message: MessageDb
):
    confirmations = [
        MessageConfirmationDb(
            item_hash=fixture_message.item_hash,
            tx=ChainTxDb(
                hash="0xdeadbeef",
                chain=Chain.ETH,
                height=1000,
                datetime=pytz.utc.localize(dt.datetime(2022, 10, 1)),
                publisher="0xabadbabe",
            ),
        ),
        MessageConfirmationDb(
            item_hash=fixture_message.item_hash,
            tx=ChainTxDb(
                hash="0x8badf00d",
                chain=Chain.ETH,
                height=1020,
                datetime=pytz.utc.localize(dt.datetime(2022, 10, 2)),
                publisher="0x0bobafed",
            ),
        ),
    ]

    fixture_message.confirmations = confirmations

    async with session_factory() as session:
        session.add(fixture_message)
        await session.commit()

    async with session_factory() as session:
        fetched_message = await get_message_by_item_hash(
            session=session, item_hash=fixture_message.item_hash
        )

    assert fetched_message is not None
    compare_messages(expected=fixture_message, actual=fetched_message)

    assert fetched_message.confirmed

    confirmations_by_hash = {confirmation.tx.hash: confirmation for confirmation in confirmations}
    for confirmation in fetched_message.confirmations:
        original = confirmations_by_hash[confirmation.tx.hash]
        assert confirmation.item_hash == original.item_hash
        assert confirmation.tx_hash == original.tx_hash
        assert confirmation.tx.hash == original.tx.hash
        assert confirmation.tx.chain == original.tx.chain
        assert confirmation.tx.height == original.tx.height
        assert confirmation.tx.datetime == original.tx.datetime
        assert confirmation.tx.publisher == original.tx.publisher


async def test_upsert_query_confirmation(session_factory: DbSessionFactory):
    # TODO
    assert False


async def test_upsert_query_message(session_factory: DbSessionFactory):
    # TODO
    assert False
