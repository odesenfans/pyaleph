from pymongo import ASCENDING, DESCENDING, IndexModel

from aleph.model.base import BaseClass


class PendingMessage(BaseClass):
    """Those messages have been received but their
    content will be retrieved later."""

    COLLECTION = "pending_messages"

    INDEXES = [
        IndexModel([("message.item_hash", ASCENDING)]),
        #    IndexModel([("message.sender", ASCENDING)]),
        #    IndexModel([("message.item_type", ASCENDING)]),
        IndexModel([("source.chain_name", ASCENDING)]),
        #    IndexModel([("source.height", ASCENDING)]),
        IndexModel([("message.time", ASCENDING)]),
        IndexModel([("retries", ASCENDING), ("message.time", ASCENDING)]),
    ]


async def pending_messages_count(message_type=None, source_chain=None):
    find_params = {}
    if message_type is not None:
        find_params = {"message.item_type": message_type}
    if source_chain is not None:
        find_params["source.chain_name"] = source_chain

    return await PendingMessage.collection.count_documents(find_params)
