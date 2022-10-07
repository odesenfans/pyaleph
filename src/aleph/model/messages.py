import logging
from typing import List, Optional

from pymongo import ASCENDING, DESCENDING, IndexModel

from aleph.model.base import BaseClass

logger = logging.getLogger(__name__)


class Message(BaseClass):
    COLLECTION = "messages"
    INDEXES = [  # Index("hash", unique=True),
        IndexModel(
            [
                ("item_hash", ASCENDING),
                ("chain", ASCENDING),
                ("sender", ASCENDING),
                ("type", ASCENDING),
            ],
            unique=True,
        ),
        IndexModel([("item_hash", ASCENDING)]),  # Content IPFS hash
        IndexModel([("tx_hash", ASCENDING)]),  # TX Hash (if there is one)
        IndexModel([("sender", ASCENDING)]),
        IndexModel([("channel", ASCENDING)]),
        IndexModel([("content.address", ASCENDING)]),
        IndexModel([("content.item_hash", ASCENDING)]),
        IndexModel([("content.key", ASCENDING)]),
        IndexModel([("content.ref", ASCENDING)]),
        IndexModel([("content.type", ASCENDING)]),
        IndexModel([("content.content.tags", ASCENDING)]),
        #    IndexModel([("content.time", ASCENDING)]),
        IndexModel([("time", DESCENDING)]),
        IndexModel([("time", ASCENDING)]),
        IndexModel([("type", ASCENDING)]),
        IndexModel(
            [("type", ASCENDING), ("content.address", ASCENDING), ("time", DESCENDING)]
        ),
        IndexModel(
            [
                ("type", ASCENDING),
                ("content.address", ASCENDING),
                ("content.key", ASCENDING),
                ("time", DESCENDING),
            ]
        ),
        IndexModel([("type", ASCENDING), ("content.type", ASCENDING)]),
        IndexModel(
            [
                ("type", ASCENDING),
                ("content.type", ASCENDING),
                ("content.content.tags", ASCENDING),
            ]
        ),
        #    IndexModel([("chain", ASCENDING)]),
        #    IndexModel([("confirmations.chain", ASCENDING)]),
        #    IndexModel([("confirmations.height", ASCENDING)]),
        #    IndexModel([("confirmations.height", DESCENDING)]),
        IndexModel([("confirmed", DESCENDING)]),
    ]


async def get_computed_address_aggregates(
    address_list: Optional[List[str]] = None,
    key_list: Optional[List[str]] = None,
    limit: int = 100,
):
    aggregate = [
        {"$match": {"type": "AGGREGATE"}},
        {"$sort": {"time": -1}},
        {"$limit": limit},
        {"$match": {"content.content": {"$type": "object"}}},
        {"$match": {"content.content": {"$not": {"$type": "array"}}}},
        {
            "$project": {
                "time": 1,
                "content.address": 1,
                "content.key": 1,
                "content.content": 1,
            }
        },
        {"$sort": {"time": 1}},
        {
            "$group": {
                "_id": {"address": "$content.address", "key": "$content.key"},
                "content": {"$mergeObjects": "$content.content"},
            }
        },
        {
            "$group": {
                "_id": "$_id.address",
                "items": {"$push": {"k": "$_id.key", "v": "$content"}},
            }
        },
        {"$addFields": {"address": "$_id", "contents": {"$arrayToObject": "$items"}}},
        {"$project": {"_id": 0, "address": 1, "contents": 1}},
    ]

    # Note: type-ignore statements are required because mypy does not understand that
    # the nested dictionaries we declared just above are dictionaries.
    if address_list is not None:
        if len(address_list) > 1:
            aggregate[0]["$match"]["content.address"] = {"$in": address_list}  # type: ignore
        else:
            aggregate[0]["$match"]["content.address"] = address_list[0]  # type: ignore

    if key_list is not None:
        if len(key_list) > 1:
            aggregate[0]["$match"]["content.key"] = {"$in": key_list}  # type: ignore
        else:
            aggregate[0]["$match"]["content.key"] = key_list[0]  # type: ignore

    results = Message.collection.aggregate(aggregate)

    return {result["address"]: result["contents"] async for result in results}


async def get_merged_posts(filters, sort=None, limit=100, skip=0, amend_limit=1):
    if sort is None:
        sort = {"confirmed": 1, "time": -1, "confirmations.height": -1}

    aggregate = [
        {"$match": {"type": "POST", **filters}},
        {"$sort": sort},
        {"$skip": skip},
        {"$limit": limit},
        {
            "$addFields": {
                "original_item_hash": "$item_hash",
                "original_signature": "$signature",
                "original_tx_hash": "$tx_hash",
                "original_type": "$content.type",
                "hash": "$item_hash",
                "original_ref": "$content.ref",
            }
        },
        {
            "$lookup": {
                "from": "messages",
                "let": {
                    "item_hash": "$item_hash",
                    "tx_hash": "$tx_hash",
                    "address": "$content.address",
                },
                "pipeline": [
                    {
                        "$match": {
                            "$and": [
                                {"type": "POST"},
                                {"content.type": "amend"},
                                # {'content.ref': {'$in': ['$$item_hash',
                                #                         '$$tx_hash']}}
                                {
                                    "$expr": {
                                        "$and": [
                                            {"$eq": ["$content.ref", "$$item_hash"]},
                                            {"$eq": ["$content.address", "$$address"]},
                                        ]
                                    }
                                },
                            ]
                        }
                    },
                    {"$sort": {"confirmed": 1, "confirmations.height": -1, "time": -1}},
                    {"$limit": amend_limit},
                ],
                "as": "amends",
            }
        },
        {
            "$replaceRoot": {
                "newRoot": {
                    "$mergeObjects": ["$$ROOT", {"$arrayElemAt": ["$amends", 0]}]
                }
            }
        },
        {"$project": {"_id": 0, "amends": 0}},
        {"$replaceRoot": {"newRoot": {"$mergeObjects": ["$$ROOT", "$content"]}}},
    ]

    return Message.collection.aggregate(aggregate)
