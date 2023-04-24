from aiohttp import web

from aleph.db.accessors.files import upsert_file
from aleph.types.files import FileType
from aleph.web.controllers.app_state_getters import (
    get_ipfs_service_from_request,
    get_session_factory_from_request,
)
from aleph.web.controllers.utils import multidict_proxy_to_io


async def ipfs_add_file(request: web.Request):
    ipfs_service = get_ipfs_service_from_request(request)
    if ipfs_service is None:
        raise web.HTTPForbidden(reason="IPFS is disabled on this node")

    session_factory = get_session_factory_from_request(request)

    # No need to pin it here anymore.
    post = await request.post()
    output = await ipfs_service.add_file(multidict_proxy_to_io(post))

    with session_factory() as session:
        upsert_file(
            session=session,
            file_hash=output["Hash"],
            size=output["Size"],
            file_type=FileType.FILE,
        )
        session.commit()

    output = {
        "status": "success",
        "hash": output["Hash"],
        "name": output["Name"],
        "size": output["Size"],
    }
    return web.json_response(output)
