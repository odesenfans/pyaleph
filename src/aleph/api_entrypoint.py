import logging
from typing import Dict, Optional

import uvicorn
from fastapi import FastAPI

import aleph.config
from aleph import __version__
from aleph.web.controllers_fastapi.info import router as info_router

LOGGER = logging.getLogger(__name__)


def run_fastapi_server(
    config_values: Dict,
    host: str,
    port: int,
    enable_sentry: bool = True,
    extra_web_config: Optional[Dict] = None,
):
    config = aleph.config.app_config
    config.load_values(config_values)

    uvicorn.run("aleph.api_entrypoint:app", port=port, log_level="info")


def create_fastapi_app():
    app = FastAPI(title="Aleph.im Core Channel Node API", version=__version__)

    app.include_router(info_router)

    return app


app = create_fastapi_app()
