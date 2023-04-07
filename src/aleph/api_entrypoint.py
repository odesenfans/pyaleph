import logging
from typing import Dict, Optional

import uvicorn
from fastapi import FastAPI

LOGGER = logging.getLogger(__name__)


def run_fastapi_server(
    config_values: Dict,
    host: str,
    port: int,
    enable_sentry: bool = True,
    extra_web_config: Optional[Dict] = None,
):
    uvicorn.run("aleph.api_entrypoint:app", port=port, log_level="info")


def create_fastapi_app():
    app = FastAPI()

    return app


app = create_fastapi_app()
