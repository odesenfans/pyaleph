import asyncio
import datetime as dt
import json
import logging
from enum import Enum
from typing import List, Sequence, Tuple, Type

import aiohttp
from aleph_message.models import Chain, MessageType, StoreContent, ItemType
from aleph_pytezos.crypto.key import Key
from configmanager import Config

from aleph.chains.common import get_verification_buffer
from aleph.chains.tx_context import TxContext
from aleph.exceptions import InvalidMessageError
from aleph.model.chains import Chain as ChainDb
from aleph.model.pending import PendingMessage
from aleph.register_chain import register_verifier, register_incoming_worker
from aleph.schemas.chains.tezos_indexer_response import (
    IndexerResponse,
    SyncStatus,
    IndexerMessageEvent,
)
from aleph.schemas.pending_messages import (
    BasePendingMessage,
    PendingStoreMessage,
    parse_message_content,
    get_message_cls,
)
from aleph.utils import get_sha256

LOGGER = logging.getLogger(__name__)
CHAIN_NAME = "TEZOS"

# Default dApp URL for Micheline-style signatures
DEFAULT_DAPP_URL = "aleph.im"


class TezosSignatureType(str, Enum):
    RAW = "raw"
    MICHELINE = "micheline"


def timestamp_to_iso_8601(timestamp: float) -> str:
    """
    Returns the timestamp formatted to ISO-8601, JS-style.

    Compared to the regular `isoformat()`, this function only provides precision down
    to milliseconds and prints a "Z" instead of +0000 for UTC.
    This format is typically used by JavaScript applications, like our TS SDK.

    Example: 2022-09-23T14:41:19.029Z

    :param timestamp: The timestamp to format.
    :return: The formatted timestamp.
    """

    return (
        dt.datetime.utcfromtimestamp(timestamp).isoformat(timespec="milliseconds") + "Z"
    )


def micheline_verification_buffer(
    verification_buffer: bytes,
    timestamp: float,
    dapp_url: str,
) -> bytes:
    """
    Computes the verification buffer for Micheline-type signatures.

    This verification buffer is used when signing data with a Tezos web wallet.
    See https://tezostaquito.io/docs/signing/#generating-a-signature-with-beacon-sdk.

    :param verification_buffer: The original (non-Tezos) verification buffer for the Aleph message.
    :param timestamp: Timestamp of the message.
    :param dapp_url: The URL of the dApp, for use as part of the verification buffer.
    :return: The verification buffer used for the signature by the web wallet.
    """

    prefix = b"Tezos Signed Message:"
    timestamp = timestamp_to_iso_8601(timestamp).encode("utf-8")

    payload = b" ".join(
        (prefix, dapp_url.encode("utf-8"), timestamp, verification_buffer)
    )
    hex_encoded_payload = payload.hex()
    payload_size = str(len(hex_encoded_payload)).encode("utf-8")

    return b"\x05" + b"\x01\x00" + payload_size + payload


def get_tezos_verification_buffer(
    message: BasePendingMessage, signature_type: TezosSignatureType, dapp_url: str
) -> bytes:
    verification_buffer = get_verification_buffer(message)

    if signature_type == TezosSignatureType.RAW:
        return verification_buffer
    elif signature_type == TezosSignatureType.MICHELINE:
        return micheline_verification_buffer(
            verification_buffer, message.time, dapp_url
        )

    raise ValueError(f"Unsupported signature type: {signature_type}")


async def verify_signature(message: BasePendingMessage) -> bool:
    """
    Verifies the cryptographic signature of a message signed with a Tezos key.
    """

    if message.signature is None:
        LOGGER.warning("'%s': missing signature.", message.item_hash)
        return False

    try:
        signature_dict = json.loads(message.signature)
    except json.JSONDecodeError:
        LOGGER.warning("Signature field for Tezos message is not JSON deserializable.")
        return False

    try:
        signature = signature_dict["signature"]
        public_key = signature_dict["publicKey"]
    except KeyError as e:
        LOGGER.warning("'%s' key missing from Tezos signature dictionary.", e.args[0])
        return False

    signature_type = TezosSignatureType(signature_dict.get("signingType", "raw"))
    dapp_url = signature_dict.get("dAppUrl", DEFAULT_DAPP_URL)

    key = Key.from_encoded_key(public_key)
    # Check that the sender ID is equal to the public key hash
    public_key_hash = key.public_key_hash()

    if message.sender != public_key_hash:
        LOGGER.warning(
            "Sender ID (%s) does not match public key hash (%s)",
            message.sender,
            public_key_hash,
        )

    verification_buffer = get_tezos_verification_buffer(
        message, signature_type, dapp_url
    )

    # Check the signature
    try:
        key.verify(signature, verification_buffer)
    except ValueError:
        LOGGER.warning("Received message with bad signature from %s" % message.sender)
        return False

    return True


register_verifier(CHAIN_NAME, verify_signature)


def make_graphql_status_query():
    return "{indexStatus {status}}"


def make_graphql_query(
    sync_contract_address: str, event_type: str, limit: int, skip: int
):
    return """
{
  indexStatus {
    oldestBlock
    recentBlock
    status
  }
  stats(address: "%s") {
    totalEvents
  }
  events(limit: %d, skip: %d, source: "%s", type: "%s") {
    source
    timestamp
    blockHash
    blockLevel
    type
    payload
  }
}
""" % (
        sync_contract_address,
        limit,
        skip,
        sync_contract_address,
        event_type,
    )


async def get_indexer_status(http_session: aiohttp.ClientSession) -> SyncStatus:
    response = await http_session.post("/", json={"query": make_graphql_status_query()})
    response.raise_for_status()
    response_json = await response.json()

    return SyncStatus(response_json["data"]["indexStatus"]["status"])


async def fetch_messages(
    http_session: aiohttp.ClientSession,
    sync_contract_address: str,
    event_type: str,
    limit: int,
    skip: int,
) -> IndexerResponse[IndexerMessageEvent]:

    query = make_graphql_query(
        limit=limit,
        skip=skip,
        sync_contract_address=sync_contract_address,
        event_type=event_type,
    )

    response = await http_session.post("/", json={"query": query})
    response.raise_for_status()
    response_json = await response.json()

    return IndexerResponse[IndexerMessageEvent].parse_obj(response_json)


def indexer_event_to_aleph_message(
    indexer_event: IndexerMessageEvent,
) -> Tuple[BasePendingMessage, TxContext]:

    if (message_type_str := indexer_event.payload.message_type) == "STORE_IPFS":
        content = StoreContent(
            address=indexer_event.payload.addr,
            time=indexer_event.payload.timestamp,
            item_type=ItemType.ipfs,
            item_hash=indexer_event.payload.message_content,
        )
        item_content = content.json()
        message_type = MessageType.store
        message_cls: Type[BasePendingMessage] = PendingStoreMessage

    else:
        try:
            message_type = MessageType(message_type_str)
        except ValueError as e:
            raise InvalidMessageError(
                f"Unsupported message type: {message_type_str}"
            ) from e

        item_content = indexer_event.payload.message_content
        try:
            content = parse_message_content(
                message_type=MessageType(message_type),
                content_dict=json.loads(item_content),
            )
        except json.JSONDecodeError as e:
            raise InvalidMessageError(
                f"Message content is not JSON: {item_content}"
            ) from e

        message_cls = get_message_cls(message_type)

    item_hash = get_sha256(item_content)

    pending_message = message_cls(
        item_hash=item_hash,
        sender=indexer_event.payload.addr,
        chain=Chain.TEZOS,
        signature=None,
        type=message_type,
        item_content=item_content,
        content=content,
        item_type=ItemType.inline,
        time=indexer_event.timestamp.timestamp(),
        channel=None,
    )

    tx_context = TxContext(
        chain_name=Chain.TEZOS,
        tx_hash=indexer_event.block_hash,
        height=indexer_event.block_level,
        time=indexer_event.timestamp.timestamp(),
        publisher=indexer_event.source,
    )

    return pending_message, tx_context


async def extract_aleph_messages_from_indexer_response(
    indexer_response: IndexerResponse[IndexerMessageEvent],
) -> List[Tuple[BasePendingMessage, TxContext]]:

    events = indexer_response.data.events
    pending_messages = []

    for event in events:
        try:
            pending_messages.append(indexer_event_to_aleph_message(event))
        except InvalidMessageError as e:
            LOGGER.warning("Invalid on-chain message: %s", str(e))
            continue

    return pending_messages


async def insert_pending_messages(
    pending_messages: Sequence[Tuple[BasePendingMessage, TxContext]]
):
    for pending_message, tx_context in pending_messages:
        await PendingMessage.collection.insert_one(
            {
                "message": pending_message.dict(exclude={"content"}),
                "source": dict(
                    chain_name=tx_context.chain_name,
                    tx_hash=tx_context.tx_hash,
                    height=tx_context.height,
                    check_message=False,
                ),
            }
        )


async def fetch_incoming_messages(config: Config):

    indexer_url = config.tezos.indexer_url.value

    async with aiohttp.ClientSession(indexer_url) as http_session:
        status = await get_indexer_status(http_session)

        if status != SyncStatus.SYNCED:
            LOGGER.warning("Tezos indexer is not yet synced, waiting until it is")
            return

        last_committed_height = (await ChainDb.get_last_height(Chain.TEZOS)) or 0
        limit = 100

        try:
            while True:
                indexer_response_data = await fetch_messages(
                    http_session,
                    sync_contract_address=config.tezos.sync_contract.value,
                    event_type="MessageEvent",
                    limit=limit,
                    skip=last_committed_height,
                )
                pending_messages = await extract_aleph_messages_from_indexer_response(
                    indexer_response_data
                )
                await insert_pending_messages(pending_messages)

                last_committed_height += limit
                if (
                    last_committed_height
                    >= indexer_response_data.data.stats.total_events
                ):
                    last_committed_height = (
                        indexer_response_data.data.stats.total_events
                    )
                    break

        finally:
            await ChainDb.set_last_height(
                chain=Chain.TEZOS, height=last_committed_height
            )


async def tezos_sync_worker(config):
    if config.tezos.enabled.value:
        while True:
            try:
                await fetch_incoming_messages(config)

            except Exception:
                LOGGER.exception(
                    "An unexpected exception occurred, "
                    "relaunching Tezos message sync in 10 seconds"
                )
            await asyncio.sleep(10)


register_incoming_worker(CHAIN_NAME, tezos_sync_worker)
