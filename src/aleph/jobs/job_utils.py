from typing import Dict

import aleph.config
from aleph.model import init_db_globals
from aleph.services.ipfs.common import init_ipfs_globals
from aleph.services.p2p import init_p2p_client
from configmanager import Config
import asyncio

async def do_something():
    ...


def prepare_subprocess(config_values: Dict) -> Config:
    """
    Prepares all the global variables (sigh) needed to run an Aleph subprocess.

    :param config_values: Dictionary of config values, as provided by the main process.
    :returns: The application configuration object, out of convenience.
    """

    app_config = aleph.config.app_config
    app_config.load_values(config_values)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(do_something())

    init_db_globals(app_config, loop)
    init_ipfs_globals(app_config)
    _ = init_p2p_client(app_config)

    return loop, app_config
