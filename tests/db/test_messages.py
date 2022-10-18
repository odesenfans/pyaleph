import datetime as dt

import pytest
import pytz
from aleph_message.models import Chain, MessageType, ItemType
from sqlalchemy import select

from aleph.db.accessors.messages import (
    get_message_by_item_hash,
    get_unconfirmed_messages,
    message_exists,
    forget_message,
    get_message_status,
    append_to_forgotten_by,
    get_forgotten_message,
    reject_message,
)
from aleph.db.models import (
    MessageDb,
    MessageConfirmationDb,
    ChainTxDb,
    MessageStatusDb,
    ForgottenMessageDb,
)
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.channel import Channel
from aleph.types.db_session import DbSessionFactory
from aleph.types.message_status import MessageStatus, InvalidSignature


@pytest.fixture
def fixture_message() -> MessageDb:
    # TODO: use a valid message, this one has incorrect signature, size, etc.

    sender = "0x51A58800b26AA1451aaA803d1746687cB88E0500"
    return MessageDb(
        item_hash="aea68aac5f4dc6e6b813fc5de9e6c17d3ef1b03e77eace15398405260baf3ce4",
        chain=Chain.ETH,
        sender=sender,
        signature="0x705ca1365a0b794cbfcf89ce13239376d0aab0674d8e7f39965590a46e5206a664bc4b313f3351f313564e033c9fe44fd258492dfbd6c36b089677d73224da0a1c",
        type=MessageType.aggregate,
        item_content='{"address": "0x51A58800b26AA1451aaA803d1746687cB88E0500", "key": "my-aggregate", "time": 1664999873, "content": {"easy": "as", "a-b": "c"}}',
        content={
            "address": sender,
            "key": "my-aggregate",
            "time": 1664999873,
            "content": {"easy": "as", "a-b": "c"},
        },
        item_type=ItemType.inline,
        size=2000,
        time=pytz.utc.localize(dt.datetime.utcfromtimestamp(1664999872)),
        channel=Channel("CHANEL-N5"),
    )


def assert_messages_equal(expected: MessageDb, actual: MessageDb):
    assert actual.item_hash == expected.item_hash
    assert actual.chain == expected.chain
    assert actual.sender == expected.sender
    assert actual.signature == expected.signature
    assert actual.type == expected.type
    assert actual.content == expected.content
    assert actual.item_type == expected.item_type
    assert actual.size == expected.size
    assert actual.time == expected.time
    assert actual.channel == expected.channel


@pytest.mark.asyncio
async def test_get_message(
    session_factory: DbSessionFactory, fixture_message: MessageDb
):
    with session_factory() as session:
        session.add(fixture_message)
        session.commit()

    with session_factory() as session:
        fetched_message = await get_message_by_item_hash(
            session=session, item_hash=fixture_message.item_hash
        )

    assert fetched_message is not None
    assert_messages_equal(expected=fixture_message, actual=fetched_message)

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

    with session_factory() as session:
        session.add(fixture_message)
        session.commit()

    with session_factory() as session:
        fetched_message = await get_message_by_item_hash(
            session=session, item_hash=fixture_message.item_hash
        )

    assert fetched_message is not None
    assert_messages_equal(expected=fixture_message, actual=fetched_message)

    assert fetched_message.confirmed

    confirmations_by_hash = {
        confirmation.tx.hash: confirmation for confirmation in confirmations
    }
    for confirmation in fetched_message.confirmations:
        original = confirmations_by_hash[confirmation.tx.hash]
        assert confirmation.item_hash == original.item_hash
        assert confirmation.tx_hash == original.tx_hash
        assert confirmation.tx.hash == original.tx.hash
        assert confirmation.tx.chain == original.tx.chain
        assert confirmation.tx.height == original.tx.height
        assert confirmation.tx.datetime == original.tx.datetime
        assert confirmation.tx.publisher == original.tx.publisher


@pytest.mark.asyncio
async def test_message_exists(session_factory: DbSessionFactory, fixture_message):
    with session_factory() as session:
        assert not await message_exists(
            session=session, item_hash=fixture_message.item_hash
        )

        session.add(fixture_message)
        session.commit()

        assert await message_exists(
            session=session, item_hash=fixture_message.item_hash
        )


@pytest.mark.asyncio
async def test_upsert_query_confirmation(session_factory: DbSessionFactory):
    # TODO
    assert False


@pytest.mark.asyncio
async def test_upsert_query_message(session_factory: DbSessionFactory):
    # TODO
    assert False


@pytest.mark.asyncio
async def test_get_unconfirmed_messages(
    session_factory: DbSessionFactory, fixture_message: MessageDb
):
    with session_factory() as session:
        session.add(fixture_message)
        session.commit()

    with session_factory() as session:
        unconfirmed_messages = list(await get_unconfirmed_messages(session))

    assert len(unconfirmed_messages) == 1
    assert_messages_equal(fixture_message, unconfirmed_messages[0])

    # Confirm the message and check that it is not returned anymore
    tx = ChainTxDb(
        hash="1234",
        chain=Chain.SOL,
        height=8000,
        datetime=timestamp_to_datetime(1664999900),
        publisher="0xabadbabe",
    )
    with session_factory() as session:
        session.add(tx)
        session.add(
            MessageConfirmationDb(item_hash=fixture_message.item_hash, tx_hash=tx.hash)
        )
        session.commit()

    with session_factory() as session:
        # Check that the message is now ignored
        unconfirmed_messages = list(await get_unconfirmed_messages(session))
        assert unconfirmed_messages == []

        # Check that it is also ignored when the chain parameter is specified
        unconfirmed_messages = list(
            await get_unconfirmed_messages(session, chain=tx.chain)
        )
        assert unconfirmed_messages == []

        # Check that it reappears if we specify a different chain
        unconfirmed_messages = list(
            await get_unconfirmed_messages(session, chain=Chain.TEZOS)
        )
        assert len(unconfirmed_messages) == 1
        assert_messages_equal(fixture_message, unconfirmed_messages[0])

        # Check that the limit parameter is respected
        unconfirmed_messages = list(
            await get_unconfirmed_messages(session, chain=Chain.TEZOS, limit=0)
        )
        assert unconfirmed_messages == []


@pytest.mark.asyncio
async def test_get_distinct_channels(session_factory: DbSessionFactory):
    assert False


@pytest.mark.asyncio
async def test_forget_message(
    session_factory: DbSessionFactory, fixture_message: MessageDb
):
    with session_factory() as session:
        session.add(fixture_message)
        session.add(
            MessageStatusDb(
                item_hash=fixture_message.item_hash, status=MessageStatus.PROCESSED
            )
        )
        session.commit()

    forget_message_hash = (
        "d06251c954d4c75476c749e80b8f2a4962d20282b28b3e237e30b0a76157df2d"
    )

    with session_factory() as session:
        await forget_message(
            session=session,
            item_hash=fixture_message.item_hash,
            forget_message_hash=forget_message_hash,
        )
        session.commit()

        message_status = await get_message_status(
            session=session, item_hash=fixture_message.item_hash
        )
        assert message_status
        assert message_status.status == MessageStatus.FORGOTTEN

        # Assert that the message is not present in messages anymore
        message = await get_message_by_item_hash(
            session=session, item_hash=fixture_message.item_hash
        )
        assert message is None

        # Assert that the metadata was inserted properly in forgotten_messages
        forgotten_message = await get_forgotten_message(
            session=session, item_hash=fixture_message.item_hash
        )
        assert forgotten_message

        assert forgotten_message.item_hash == fixture_message.item_hash
        assert forgotten_message.type == fixture_message.type
        assert forgotten_message.chain == fixture_message.chain
        assert forgotten_message.sender == fixture_message.sender
        assert forgotten_message.signature == fixture_message.signature
        assert forgotten_message.item_type == fixture_message.item_type
        assert forgotten_message.time == fixture_message.time
        assert forgotten_message.channel == fixture_message.channel
        assert forgotten_message.forgotten_by == [forget_message_hash]

        # Now, add a hash to forgotten_by
        new_forget_message_hash = (
            "2aa1f44199181110e0c6b79ccc5e40ceaf20eac791dcfcd1b4f8f2f32b2d8502"
        )

        await append_to_forgotten_by(
            session=session,
            forgotten_message_hash=fixture_message.item_hash,
            forget_message_hash=new_forget_message_hash,
        )
        session.commit()

        forgotten_message = await get_forgotten_message(
            session=session, item_hash=fixture_message.item_hash
        )
        assert forgotten_message
        assert forgotten_message.forgotten_by == [
            forget_message_hash,
            new_forget_message_hash,
        ]

@pytest.mark.asyncio
async def test_reject_message(
    session_factory: DbSessionFactory, fixture_message: MessageDb
):
    with session_factory() as session:
        session.add(fixture_message)
        session.add(
            MessageStatusDb(
                item_hash=fixture_message.item_hash, status=MessageStatus.FETCHED
            )
        )
        session.commit()

    with session_factory() as session:
        await reject_message(
            session=session,
            item_hash=fixture_message.item_hash,
            exception=InvalidSignature("Signature does not match"),
        )
        session.commit()
