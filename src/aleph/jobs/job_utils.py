import asyncio
from itertools import groupby
from typing import Awaitable, Callable, List
from typing import Dict
from typing import Iterable, Tuple

import pymongo.errors
from configmanager import Config

import aleph.config
from aleph.model import init_db_globals
from aleph.model.db_bulk_operation import DbBulkOperation
from aleph.services.ipfs.common import init_ipfs_globals
from aleph.services.p2p import init_p2p_client


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

    for collection, operations in groupby(
        sorted_operations, lambda op: op.collection
    ):
        # Keep capped operations for later
        if collection.is_capped():
            capped_collection_operations.extend(operations)
            continue

        mongo_ops = [op.operation for op in operations]
        await collection.collection.bulk_write(mongo_ops)

    # Ugly hack: we process capped collections after the rest, and process changes
    # one by one. The reason for that is that we only have one capped collection
    # at the moment (CappedMessage), and we have no way at the moment to avoid writing
    # the same message several times from different channels. This would fail in
    # the normal flow because capped collections do not support updates that modify
    # the size of a document. Instead, we process updates one by one and just ignore
    # the ones that fail, as it means the message is already inserted.
    for operation in capped_collection_operations:
        try:
            await operation.collection.collection.bulk_write([operation.operation])
        except pymongo.errors.BulkWriteError:
            pass


async def gather_and_perform_db_operations(
    tasks: List[Awaitable[List[DbBulkOperation]]],
    on_error: Callable[[BaseException], None],
) -> None:
    """
    Processes the result of the pending TX/message tasks.

    Gathers the results of the tasks passed in input, handles exceptions
    and performs DB operations.

    :param tasks: Job tasks. Each of these tasks must return a list of
                  DbBulkOperation objects.
    :param on_error: Error callback function. This function will be called
                     on each error from one of the tasks.
    """
    task_results = await asyncio.gather(*tasks, return_exceptions=True)

    errors = [op for op in task_results if isinstance(op, BaseException)]
    for error in errors:
        on_error(error)

    db_operations = (
        op
        for operations in task_results
        if not isinstance(operations, BaseException)
        for op in operations
    )

    await perform_db_operations(db_operations)
