from pymongo import IndexModel, HASHED

from aleph.model.base import BaseClass


class PermanentPin(BaseClass):
    """Hold information about pinned files."""

    COLLECTION = "filepins"

    INDEXES = [
        IndexModel([("multihash", HASHED)])
    ]


    @classmethod
    async def register(cls, multihash: str, reason):
        await cls.collection.update(
            {"multihash": multihash},
            {"$push": {reason}}
        )
