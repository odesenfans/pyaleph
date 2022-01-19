"""
"""

from datetime import datetime, timedelta
from typing import Optional

from pymongo import ASCENDING, DESCENDING, IndexModel

from aleph.model.base import BaseClass
from aleph.types import Protocol


class Peer(BaseClass):
    """Holds information about the chains state."""

    COLLECTION = "peers"

    INDEXES = [
        IndexModel([("type", ASCENDING)]),
        IndexModel([("address", ASCENDING)]),
        IndexModel([("last_seen", DESCENDING)]),
    ]


async def get_peers(peer_type: Optional[str] = None, hours: int = 2):
    """Returns current peers.
    TODO: handle the last seen, channel preferences, and better way of avoiding "bad contacts".
    NOTE: Currently used in jobs.
    """
    async for peer in Peer.collection.find(
        {
            "type": peer_type,
            "last_seen": {"$gt": datetime.now() - timedelta(hours=hours)},
        }
    ).sort([("last_seen", -1)]):
        yield peer["address"]


async def add_peer(address: str, peer_type: str, source: Protocol, sender: Optional[str] = None):
    await Peer.collection.replace_one(
        {"address": address, "type": peer_type},
        {
            "address": address,
            "type": peer_type,
            "last_seen": datetime.now(),
            "sender": sender,
            "source": source.value,
        },
        upsert=True,
    )
