import asyncio
import logging
from dataclasses import asdict
from typing import Dict, Annotated

import aiohttp_jinja2
from aiohttp import web
from fastapi import Depends
from starlette.responses import PlainTextResponse

from aleph.schemas.api.metrics import MetricsResponse
from aleph.services.cache.node_cache import NodeCache
from aleph.types.db_session import DbSessionFactory, DbSession
from aleph.web.controllers.app_state_getters import (
    get_node_cache_from_request,
    get_session_factory_from_request,
)
from aleph.web.controllers.metrics import format_dataclass_for_prometheus, get_metrics, Metrics
from aleph.web.controllers_fastapi.dependencies import db_session, node_cache

from .router import router, ws_router
from fastapi import WebSocket


logger = logging.getLogger(__name__)


@aiohttp_jinja2.template("index.html")
async def index(request) -> Dict:
    """Index of aleph."""

    session_factory: DbSessionFactory = get_session_factory_from_request(request)
    node_cache = get_node_cache_from_request(request)
    with session_factory() as session:
        return asdict(await get_metrics(session=session, node_cache=node_cache))


@ws_router.websocket("/status")
async def status_ws(websocket: WebSocket):
    await websocket.accept()

    ws = web.WebSocketResponse()
    await ws.prepare(request)

    session_factory: DbSessionFactory = get_session_factory_from_request(request)
    node_cache = get_node_cache_from_request(request)

    previous_status = None
    while True:
        with session_factory() as session:
            status = await get_metrics(session=session, node_cache=node_cache)

        if status != previous_status:
            try:
                await ws.send_json(asdict(status))
            except ConnectionResetError:
                logger.warning("Websocket connection reset")
                await ws.close()
                return ws
            previous_status = status

        await asyncio.sleep(2)


@router.get("/metrics", response_class=PlainTextResponse)
async def get_metrics_prometheus(
    session: Annotated[DbSession, Depends(db_session)],
    cache: Annotated[NodeCache, Depends(node_cache)],
) -> str:
    """Prometheus compatible metrics.

    Naming convention:
    https://prometheus.io/docs/practices/naming/
    """

    return format_dataclass_for_prometheus(
        await get_metrics(session=session, node_cache=cache)
    )


@router.get("/metrics.json", response_model=MetricsResponse)
async def get_metrics_json(
    session: Annotated[DbSession, Depends(db_session)],
    cache: Annotated[NodeCache, Depends(node_cache)],
) -> Metrics:
    """JSON version of the Prometheus metrics."""

    metrics = await get_metrics(session=session, node_cache=cache)
    return metrics
