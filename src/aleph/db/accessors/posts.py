from typing import Optional

from aleph.db.models.posts import PostDb


async def get_post(item_hash: str) -> Optional[PostDb]:
    ...
