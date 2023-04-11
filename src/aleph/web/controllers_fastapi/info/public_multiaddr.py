from typing import Annotated

from fastapi import Depends

from aleph.schemas.api.info import InfoPublicResponse
from aleph.services.cache.node_cache import NodeCache
from aleph.web.controllers_fastapi.dependencies import node_cache
from .router import router


@router.get("/public.json", response_model=InfoPublicResponse)
async def public_multiaddress(cache: Annotated[NodeCache, Depends(node_cache)]):
    """Broadcast public node addresses

    According to multiaddr spec https://multiformats.io/multiaddr/
    """

    public_addresses = await cache.get_public_addresses()
    return {"node_multi_addresses": public_addresses}
