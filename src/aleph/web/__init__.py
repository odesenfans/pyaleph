import json
import logging
import pprint
import time
from datetime import date, datetime, timedelta
from typing import Any
from pydantic import ValidationError

import aiohttp_cors
import aiohttp_jinja2
import jinja2
import pkg_resources
import socketio
from aiohttp import web

from aleph.web.controllers.routes import register_routes


LOGGER = logging.getLogger(__name__)


def init_cors(app: web.Application):
    # Configure default CORS settings.
    cors = aiohttp_cors.setup(
        app,
        defaults={
            "*": aiohttp_cors.ResourceOptions(
                allow_methods=["GET", "POST"],
                allow_credentials=True,
                expose_headers="*",
                allow_headers="*",
            )
        },
    )

    # Configure CORS on all routes.
    for route in list(app.router.routes()):
        if "/socket.io/" not in repr(route.resource):
            cors.add(route)


def init_sio(app: web.Application) -> socketio.AsyncServer:
    sio = socketio.AsyncServer(async_mode="aiohttp", cors_allowed_origins="*")
    sio.attach(app)
    return sio


def make_json_error(status: int, detail: Any) -> web.Response:
    return web.Response(
        status=status,
        body=json.dumps({"detail": detail}),
        content_type="application/json",
    )


async def error_middleware(app: web.Application, handler):
    async def middleware_handler(request) -> web.Response:
        try:
            return await handler(request)
        except ValidationError as e:
            return make_json_error(status=422, detail=e.json())
        except (web.HTTPClientError, web.HTTPServerError) as e:
            return make_json_error(status=e.status, detail=str(e))
        except Exception as e:
            LOGGER.warning("Request %s has failed with exception: %s", request, repr(e))
            return make_json_error(status=500, detail=str(e))

    return middleware_handler


def create_app() -> web.Application:
    app = web.Application(
        client_max_size=1024**2 * 64, middlewares=[error_middleware]
    )

    tpl_path = pkg_resources.resource_filename("aleph.web", "templates")
    jinja_loader = jinja2.ChoiceLoader(
        [
            jinja2.FileSystemLoader(tpl_path),
        ]
    )
    aiohttp_jinja2.setup(app, loader=jinja_loader)
    env = aiohttp_jinja2.get_env(app)
    env.globals.update(
        {
            "app": app,
            "date": date,
            "datetime": datetime,
            "time": time,
            "timedelta": timedelta,
            "int": int,
            "float": float,
            "len": len,
            "pprint": pprint,
        }
    )

    register_routes(app)

    init_cors(app)

    return app


app = create_app()
sio = init_sio(app)
