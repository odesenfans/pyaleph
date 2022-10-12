import datetime as dt
import json
from typing import Dict, Mapping

import pytest
import pytz
from aleph_message.models import Chain

from aleph.chains.chain_service import ChainService
from aleph.db.accessors.messages import get_message_by_item_hash
from aleph.db.models import ChainTxDb, PendingMessageDb
from aleph.handlers.message_handler import MessageHandler
from aleph.schemas.pending_messages import parse_message
from aleph.storage import StorageService
from aleph.types.db_session import DbSessionFactory

MESSAGE_DICT: Mapping = {
    "chain": "ETH",
    "channel": "TEST",
    "sender": "0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106",
    "type": "POST",
    "time": 1652803407.1179411,
    "item_type": "inline",
    "item_content": '{"address":"0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106","time":1652803407.1178224,"content":{"body":"Top 10 cutest Kodiak bears that will definitely murder you"},"type":"test"}',
    "item_hash": "85abdd0ea565ac0f282d1a86b5b3da87ed3d55426a78e9c0ec979ae58e947b9c",
    "signature": "0xfd5183273be769aaa44ea494911c9e4702fde87dd7dd5e2d5ec76c0a251654544bc98eacd33ca204a536f55f726130683cab1d1ad5ac8da1cbbf39d4d7a124401b",
}


def remove_id_key(mongodb_object: Dict) -> Dict:
    return {k: v for k, v in mongodb_object.items() if k != "_id"}


@pytest.fixture
def chain_tx() -> ChainTxDb:
    return ChainTxDb(
        hash="123",
        chain=Chain.ETH,
        height=8000,
        datetime=pytz.utc.localize(dt.datetime(2022, 10, 1)),
        publisher="0xabadbabe",
    )


def compare_chain_txs(expected: ChainTxDb, actual: ChainTxDb):
    assert actual.chain == expected.chain
    assert actual.hash == expected.hash
    assert actual.height == expected.height
    assert actual.datetime == expected.datetime
    assert actual.publisher == expected.publisher


@pytest.mark.asyncio
async def test_confirm_message(
    session_factory: DbSessionFactory,
    test_storage_service: StorageService,
    chain_tx: ChainTxDb,
):
    """
    Tests the flow of confirmation for real-time messages.
    1. We process the message unconfirmed, as if it came through the P2P
       network
    2. We process the message again, this time as it it was fetched from
       on-chain data.

    We then check that the message was correctly updated in the messages
    collection. We also check the capped messages collection used for
    the websockets.
    """

    item_hash = MESSAGE_DICT["item_hash"]
    content = json.loads(MESSAGE_DICT["item_content"])

    message_handler =MessageHandler(
        session_factory=session_factory,
        chain_service=ChainService(
            session_factory=session_factory, storage_service=test_storage_service
        ),
        storage_service=test_storage_service,
    )

    message = parse_message(MESSAGE_DICT)
    await message_handler.process_one_message(message)

    with session_factory() as session:
        message_in_db = await get_message_by_item_hash(
            session=session, item_hash=item_hash
        )

    assert message_in_db is not None
    assert message_in_db.content == content
    assert not message_in_db.confirmed

    # Now, confirm the message

    # Insert a transaction in the DB to validate the foreign key constraint
    with session_factory() as session:
        session.add(chain_tx)
        session.commit()

    await message_handler.process_one_message(message=message, chain_tx=chain_tx)

    with session_factory() as session:
        message_in_db = await get_message_by_item_hash(
            session=session, item_hash=item_hash
        )

    assert message_in_db is not None
    assert message_in_db.confirmed
    assert len(message_in_db.confirmations) == 1
    confirmation = message_in_db.confirmations[0]
    compare_chain_txs(expected=chain_tx, actual=confirmation.tx)


@pytest.mark.asyncio
async def test_process_confirmed_message(
    session_factory: DbSessionFactory,
    test_storage_service: StorageService,
    chain_tx: ChainTxDb,
):
    """
    Tests that a confirmed message coming directly from the on-chain integration flow
    is processed correctly, and that we get one confirmed entry in messages and none
    in capped messages (historical data/confirmations are not added to capped messages).
    """

    item_hash = MESSAGE_DICT["item_hash"]

    message_handler =MessageHandler(
        session_factory=session_factory,
        chain_service=ChainService(
            session_factory=session_factory, storage_service=test_storage_service
        ),
        storage_service=test_storage_service,
    )

    # Insert a transaction in the DB to validate the foreign key constraint
    with session_factory() as session:
        session.add(chain_tx)
        session.commit()

    pending_message = PendingMessageDb.from_message_dict(MESSAGE_DICT)
    await message_handler.process_one_message(message=message, chain_tx=chain_tx)

    with session_factory() as session:
        message_in_db = await get_message_by_item_hash(
            session=session, item_hash=item_hash
        )

    assert message_in_db is not None
    assert message_in_db.confirmed
    assert len(message_in_db.confirmations) == 1
    confirmation = message_in_db.confirmations[0]
    compare_chain_txs(expected=chain_tx, actual=confirmation.tx)
