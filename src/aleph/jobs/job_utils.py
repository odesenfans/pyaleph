import asyncio
import logging
import time
from itertools import groupby
from typing import Callable, cast
from typing import Dict
from typing import Iterable, Tuple
from typing import List

import pymongo.errors
from configmanager import Config

import aleph.config
from aleph.model import init_db_globals
from aleph.model.db_bulk_operation import DbBulkOperation
from aleph.model.messages import CappedMessage
from aleph.services.ipfs.common import init_ipfs_globals
from aleph.services.p2p import init_p2p_client
from aleph.toolkit.split import split_iterable


LOGGER = logging.getLogger()


def prepare_loop(config_values: Dict) -> Tuple[asyncio.AbstractEventLoop, Config]:
    """
    Prepares all the global variables (sigh) needed to run an Aleph subprocess.

    :param config_values: Dictionary of config values, as provided by the main process.
    :returns: A preconfigured event loop, and the application config for convenience.
              Use the event loop as event loop of the process as it is used by Motor. Using another
              event loop will cause DB calls to fail.
    """

    loop = asyncio.get_event_loop()

    config = aleph.config.app_config
    config.load_values(config_values)

    init_db_globals(config)
    init_ipfs_globals(config)
    _ = init_p2p_client(config)
    return loop, config


async def perform_db_operations(db_operations: Iterable[DbBulkOperation]) -> None:
    # Sort the operations by collection name before grouping and executing them.
    sorted_operations = sorted(
        db_operations,
        key=lambda op: op.collection.__name__,
    )

    capped_collection_operations: List[DbBulkOperation] = []

    for collection, operations in groupby(sorted_operations, lambda op: op.collection):
        # Keep capped operations for later
        if collection.is_capped():
            capped_collection_operations.extend(operations)
            continue

        start_time = time.time()
        mongo_ops = [op.operation for op in operations]
        await collection.collection.bulk_write(mongo_ops, ordered=False)
        LOGGER.info(
            "Wrote %d documents to collection %s in %.4f seconds",
            len(mongo_ops),
            collection.__name__,
            time.time() - start_time,
        )

    # Ugly hack: we process capped collections after the rest, and process changes
    # one by one. The reason for that is that we only have one capped collection
    # at the moment (CappedMessage), and we have no way at the moment to avoid writing
    # the same message several times from different channels. This would fail in
    # the normal flow because capped collections do not support updates that modify
    # the size of a document. Instead, we process updates one by one and just ignore
    # the ones that fail, as it means the message is already inserted.
    if capped_collection_operations:
        start_time = time.time()
        mongo_ops = [op.operation for op in capped_collection_operations]

        try:
            await CappedMessage.collection.bulk_write(mongo_ops, ordered=False)
        except pymongo.errors.BulkWriteError:
            pass

        LOGGER.info(
            "Wrote %d documents to CappedMessage in %.4f seconds",
            len(capped_collection_operations),
            time.time() - start_time,
        )


async def process_job_results(
    tasks: Iterable[asyncio.Task],  # TODO: switch to a generic type when moving to 3.9+
    on_error: Callable[[BaseException], None],
):
    """
    Processes the result of the pending TX/message tasks.

    Splits successful and failed jobs, handles exceptions and performs
    DB operations.

    :param tasks: Finished job tasks. Each of these tasks must return a list of
                  DbBulkOperation objects. It is up to the caller to determine
                  when tasks are done, for example by using asyncio.wait.
    :param on_error: Error callback function. This function will be called
                     on each error from one of the tasks.
    """
    successes, errors = split_iterable(tasks, lambda t: t.exception() is None)

    for error in errors:
        # mypy sees Optional[BaseException] otherwise
        exception = cast(BaseException, error.exception())
        on_error(exception)

    db_operations = (op for success in successes for op in success.result())

    await perform_db_operations(db_operations)
