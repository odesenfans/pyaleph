#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This is a skeleton file that can serve as a starting point for a Python
console script.

Then run `python setup.py install` which will install the command `pyaleph`
inside your current environment.
Besides console scripts, the header (i.e. until _logger...) of this file can
also be used as template for Python modules.
"""

import asyncio
import logging
import os
import sys
from multiprocessing import Manager, Process, set_start_method
from multiprocessing.managers import SyncManager
from typing import Any, Coroutine, Dict, List, Optional

import alembic.config
import alembic.command
import sentry_sdk
from aleph_message.models import MessageType
from configmanager import Config
from setproctitle import setproctitle

import aleph.config
from aleph.chains.chain_service import ChainService
from aleph.cli.args import parse_args
from aleph.db.connection import make_engine, make_session_factory, make_db_url
from aleph.exceptions import InvalidConfigException, KeyNotFoundException
from aleph.jobs import start_jobs
from aleph.jobs.job_utils import prepare_loop
from aleph.logging import setup_logging
from aleph.network import listener_tasks
from aleph.services import p2p
from aleph.services.ipfs import IpfsService
from aleph.services.ipfs.common import make_ipfs_client
from aleph.services.keys import generate_keypair, save_keys
from aleph.services.p2p import singleton, init_p2p_client
from aleph.services.storage.fileystem_engine import FileSystemStorageEngine
from aleph.storage import StorageService
from aleph.web import app

__author__ = "Moshe Malawach"
__copyright__ = "Moshe Malawach"
__license__ = "mit"

LOGGER = logging.getLogger(__name__)


def run_db_migrations(config: Config):
    db_url = make_db_url(driver="psycopg2", config=config)
    alembic_cfg = alembic.config.Config("alembic.ini")
    alembic_cfg.attributes['configure_logger'] = False
    logging.getLogger('alembic').setLevel(logging.CRITICAL)
    alembic.command.upgrade(alembic_cfg, "head", tag=db_url)


def init_shared_stats(shared_memory_manager: SyncManager) -> Dict[str, Any]:
    """
    Initializes the shared stats dictionary. This dictionary is meant to be shared
    across processes to publish internal statistics about each job.
    """
    shared_stats: Dict[str, Any] = shared_memory_manager.dict()
    # Nested dicts must also be shared dictionaries, otherwise they will not be
    # shared across processes.
    message_jobs_dict = shared_memory_manager.dict()
    for message_type in MessageType:
        message_jobs_dict[message_type] = 0
    shared_stats["message_jobs"] = message_jobs_dict

    return shared_stats


async def run_server(
    config: Config,
    host: str,
    port: int,
    shared_stats: dict,
    extra_web_config: dict,
):
    # These imports will run in different processes
    from aiohttp import web

    LOGGER.debug("Setup of runner")
    p2p_client = await init_p2p_client(config, service_name=f"api-server-{port}")

    engine = make_engine(config, echo=config.logging.level.value == logging.DEBUG)
    session_factory = make_session_factory(engine)

    ipfs_client = make_ipfs_client(config)
    ipfs_service = IpfsService(ipfs_client=ipfs_client)
    storage_service = StorageService(
        storage_engine=FileSystemStorageEngine(folder=config.storage.folder.value),
        ipfs_service=ipfs_service,
    )

    app["config"] = config
    app["extra_config"] = extra_web_config
    app["shared_stats"] = shared_stats
    app["p2p_client"] = p2p_client
    app["storage_service"] = storage_service
    app["session_factory"] = session_factory

    runner = web.AppRunner(app)
    await runner.setup()

    LOGGER.info("Starting API server on port %d...", port)
    site = web.TCPSite(runner, host, port)
    await site.start()

    # Wait forever
    while True:
        await asyncio.sleep(3600)


def run_server_coroutine(
    config_values: Dict,
    host: str,
    port: int,
    shared_stats: Dict,
    enable_sentry: bool = True,
    extra_web_config: Optional[Dict] = None,
):
    """Run the server coroutine in a synchronous way.
    Used as target of multiprocessing.Process.
    """
    setproctitle(f"pyaleph-run_server_coroutine-{port}")

    loop, config = prepare_loop(config_values)

    extra_web_config = extra_web_config or {}
    setup_logging(
        loglevel=config.logging.level.value,
        filename=f"/tmp/run_server_coroutine-{port}.log",
        max_log_file_size=config.logging.max_log_file_size.value,
    )
    if enable_sentry:
        sentry_sdk.init(
            dsn=config.sentry.dsn.value,
            traces_sample_rate=config.sentry.traces_sample_rate.value,
            ignore_errors=[KeyboardInterrupt],
        )

    # Use a try-catch-capture_exception to work with multiprocessing, see
    # https://github.com/getsentry/raven-python/issues/1110
    try:
        loop.run_until_complete(
            run_server(config, host, port, shared_stats, extra_web_config)
        )
    except Exception as e:
        if enable_sentry:
            sentry_sdk.capture_exception(e)
            sentry_sdk.flush()
        raise


async def main(args):
    """Main entry point allowing external calls

    Args:
      args ([str]): command line parameter list
    """

    args = parse_args(args)
    setup_logging(args.loglevel)

    # Generate keys and exit
    if args.generate_keys:
        LOGGER.info("Generating a key pair")
        key_pair = generate_keypair(args.print_key)
        save_keys(key_pair, args.key_dir)
        if args.print_key:
            print(key_pair.private_key.impl.export_key().decode("utf-8"))

        return

    LOGGER.info("Loading configuration")
    config = aleph.config.app_config

    if args.config_file is not None:
        LOGGER.debug("Loading config file '%s'", args.config_file)
        config.yaml.load(args.config_file)

    # CLI config values override config file values
    config.logging.level.value = args.loglevel

    # Check for invalid/deprecated config
    if "protocol" in config.p2p.clients.value:
        msg = "The 'protocol' P2P config is not supported by the current version."
        LOGGER.error(msg)
        raise InvalidConfigException(msg)

    # We only check that the private key exists.
    private_key_file_path = os.path.join(args.key_dir, "node-secret.pkcs8.der")
    if not os.path.isfile(private_key_file_path):
        msg = f"Serialized node key ({private_key_file_path}) not found."
        LOGGER.critical(msg)
        raise KeyNotFoundException(msg)

    if args.port:
        config.aleph.port.value = args.port
    if args.host:
        config.aleph.host.value = args.host

    if args.sentry_disabled:
        LOGGER.info("Sentry disabled by CLI arguments")
    elif config.sentry.dsn.value:
        sentry_sdk.init(
            dsn=config.sentry.dsn.value,
            traces_sample_rate=config.sentry.traces_sample_rate.value,
            ignore_errors=[KeyboardInterrupt],
        )
        LOGGER.info("Sentry enabled")

    config_values = config.dump_values()

    LOGGER.info("Initializing database...")
    run_db_migrations(config)
    LOGGER.info("Database initialized.")

    engine = make_engine(config, echo=args.loglevel == logging.DEBUG)
    session_factory = make_session_factory(engine)

    # TODO: find a way to prevent alembic from messing up the logging config
    setup_logging(args.loglevel)

    ipfs_service = IpfsService(ipfs_client=make_ipfs_client(config))
    storage_service = StorageService(
        storage_engine=FileSystemStorageEngine(folder=config.storage.folder.value),
        ipfs_service=ipfs_service,
    )
    chain_service = ChainService(
        session_factory=session_factory, storage_service=storage_service
    )

    set_start_method("spawn")

    with Manager() as shared_memory_manager:
        tasks: List[Coroutine] = []

        shared_stats = init_shared_stats(shared_memory_manager)
        api_servers = shared_memory_manager.list()
        singleton.api_servers = api_servers

        if not args.no_jobs:
            LOGGER.debug("Creating jobs")
            tasks += start_jobs(
                config=config,
                session_factory=session_factory,
                shared_stats=shared_stats,
                ipfs_service=ipfs_service,
                api_servers=api_servers,
                use_processes=True,
            )

        LOGGER.debug("Initializing p2p")
        p2p_client, p2p_tasks = await p2p.init_p2p(
            config=config,
            session_factory=session_factory,
            service_name="network-monitor",
            ipfs_service=ipfs_service,
            api_servers=api_servers,
        )
        tasks += p2p_tasks
        LOGGER.debug("Initialized p2p")

        LOGGER.debug("Initializing listeners")
        tasks += listener_tasks(
            config=config, session_factory=session_factory, p2p_client=p2p_client
        )
        tasks.append(chain_service.chain_event_loop(config))
        LOGGER.debug("Initialized listeners")

        # Need to be passed here otherwise it gets lost in the fork
        from aleph.services.p2p import manager as p2p_manager

        extra_web_config = {"public_adresses": p2p_manager.public_adresses}

        p1 = Process(
            target=run_server_coroutine,
            args=(
                config_values,
                config.aleph.host.value,
                config.p2p.http_port.value,
                shared_stats,
                args.sentry_disabled is False and config.sentry.dsn.value,
                extra_web_config,
            ),
        )
        p2 = Process(
            target=run_server_coroutine,
            args=(
                config_values,
                config.aleph.host.value,
                config.aleph.port.value,
                shared_stats,
                args.sentry_disabled is False and config.sentry.dsn.value,
                extra_web_config,
            ),
        )
        p1.start()
        p2.start()
        LOGGER.debug("Started processes")

        LOGGER.debug("Running event loop")
        await asyncio.gather(*tasks)


def run():
    """Entry point for console_scripts"""
    try:
        asyncio.run(main(sys.argv[1:]))
    except (KeyNotFoundException, InvalidConfigException):
        sys.exit(1)


if __name__ == "__main__":
    run()
