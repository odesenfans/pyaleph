import json
from typing import Dict, Mapping

import pytest

from aleph.chains.chain_service import ChainService
from aleph.handlers.message_handler import MessageHandler
from aleph.model.messages import CappedMessage, Message
from aleph.schemas.pending_messages import parse_message
from aleph.storage import StorageService

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


@pytest.mark.asyncio
async def test_confirm_message(
    test_db, session_factory, test_storage_service: StorageService
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

    message_handler = MessageHandler(
        session_factory=session_factory,
        chain_service=ChainService(
            session_factory=session_factory, storage_service=test_storage_service
        ),
        storage_service=test_storage_service,
    )

    message = parse_message(MESSAGE_DICT)
    await message_handler.process_one_message(message)
    message_in_db = await Message.collection.find_one({"item_hash": item_hash})

    assert message_in_db is not None
    assert message_in_db["content"] == content
    assert not message_in_db["confirmed"]

    capped_message_in_db = await CappedMessage.collection.find_one(
        {"item_hash": item_hash}
    )
    assert capped_message_in_db is not None
    assert remove_id_key(message_in_db) == remove_id_key(capped_message_in_db)

    # Now, confirm the message
    chain_name, tx_hash, height = "ETH", "123", 8000
    await message_handler.process_one_message(
        message, chain_name=chain_name, tx_hash=tx_hash, height=height
    )

    message_in_db = await Message.collection.find_one({"item_hash": item_hash})

    assert message_in_db is not None
    assert message_in_db["confirmed"]
    assert {"chain": chain_name, "hash": tx_hash, "height": height} in message_in_db[
        "confirmations"
    ]

    capped_message_after_confirmation = await CappedMessage.collection.find_one(
        {"item_hash": item_hash}
    )

    assert capped_message_after_confirmation == capped_message_in_db
    assert not capped_message_after_confirmation["confirmed"]
    assert "confirmations" not in capped_message_after_confirmation


@pytest.mark.asyncio
async def test_process_confirmed_message(
    test_db, session_factory, test_storage_service: StorageService
):
    """
    Tests that a confirmed message coming directly from the on-chain integration flow
    is processed correctly, and that we get one confirmed entry in messages and none
    in capped messages (historical data/confirmations are not added to capped messages).
    """

    item_hash = MESSAGE_DICT["item_hash"]

    message_handler = MessageHandler(
        session_factory=session_factory,
        chain_service=ChainService(
            session_factory=session_factory, storage_service=test_storage_service
        ),
        storage_service=test_storage_service,
    )

    # Confirm the message
    chain_name, tx_hash, height = "ETH", "123", 8000
    message = parse_message(MESSAGE_DICT)
    await message_handler.process_one_message(
        message, chain_name=chain_name, tx_hash=tx_hash, height=height
    )

    message_in_db = await Message.collection.find_one({"item_hash": item_hash})

    assert message_in_db is not None
    assert message_in_db["confirmed"]

    expected_confirmations = [{"chain": chain_name, "hash": tx_hash, "height": height}]
    assert message_in_db["confirmations"] == expected_confirmations

    capped_message_in_db = await CappedMessage.collection.find_one(
        {"item_hash": item_hash}
    )

    # Historical messages are not supposed to be added to capped messages
    assert capped_message_in_db is None
