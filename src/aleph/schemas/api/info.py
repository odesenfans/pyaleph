from typing import List

from pydantic import BaseModel


class InfoPublicResponse(BaseModel):
    node_multi_addresses: List[str]
