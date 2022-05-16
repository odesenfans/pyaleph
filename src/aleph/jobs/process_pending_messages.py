"""
Job in charge of (re-) processing Aleph messages waiting in the pending queue.
"""

import asyncio
from functools import partial
from logging import getLogger
from typing import List, Dict, Tuple, Set, Iterable, Callable

import sentry_sdk
from pymongo import DeleteOne, DeleteMany, ASCENDING
from setproctitle import setproctitle

from aleph.chains.common import incoming, IncomingStatus
from aleph.logging import setup_logging
from aleph.model.db_bulk_operation import DbBulkOperation
from aleph.model.pending import PendingMessage
from aleph.services.p2p import singleton
from .job_utils import prepare_loop, perform_db_operations
from aleph.toolkit.split import split_iterable

LOGGER = getLogger("jobs.pending_messages")


async def handle_pending_message(
    pending: Dict,
    seen_ids: Dict[Tuple, int],
) -> List[DbBulkOperation]:
    status, operations = await incoming(
        pending["message"],
        chain_name=pending["source"].get("chain_name"),
        tx_hash=pending["source"].get("tx_hash"),
        height=pending["source"].get("height"),
        seen_ids=seen_ids,
        check_message=pending["source"].get("check_message", True),
        retrying=True,
        existing_id=pending["_id"],
    )

    if status != IncomingStatus.RETRYING_LATER:
        operations.append(
            DbBulkOperation(PendingMessage, DeleteOne({"_id": pending["_id"]}))
        )

    return operations


async def process_job_results(
    tasks: Iterable[asyncio.Task],  # TODO: switch to a generic type when moving to 3.9+
    on_error: Callable[[BaseException], None],
):
    successes, errors = split_iterable(tasks, lambda t: t.exception() is None)

    for error in errors:
        on_error(error.exception())

    db_operations = (op for success in successes for op in success.result())

    await perform_db_operations(db_operations)


async def process_message_job_results(message_tasks: Set[asyncio.Task]):
    await process_job_results(
        message_tasks,
        on_error=partial(LOGGER.exception, "Error while processing message: %s"),
    )


def validate_pending_message(pending: Dict):
    if pending.get("message") is None:
        raise ValueError(
            f"Found PendingMessage with empty message: {pending['_id']}. "
            "This should be caught before insertion"
        )

    if not isinstance(pending["message"], dict):
        raise ValueError(
            f"Pending message is not a dictionary and cannot be read: {pending['_id']}."
        )


async def process_pending_messages(shared_stats: Dict):
    """
    Processes all the messages in the pending message queue.
    """

    seen_ids: Dict[Tuple, int] = dict()
    find_params: Dict = {}

    message_tasks = set()
    max_concurrent_tasks = 10

    while await PendingMessage.collection.count_documents(find_params):
        async for pending in PendingMessage.collection.find(find_params).sort(
            [("retries", ASCENDING), ("message.time", ASCENDING)]
        ).batch_size(max_concurrent_tasks):
            LOGGER.debug(
                f"retry_message_job len_seen_ids={len(seen_ids)} "
                f"len_message_tasks={len(message_tasks)}"
            )

            validate_pending_message(pending)

            shared_stats["retry_messages_job_seen_ids"] = len(seen_ids)
            shared_stats["retry_messages_job_tasks"] = len(message_tasks)

            if len(message_tasks) == max_concurrent_tasks:
                done, message_tasks = await asyncio.wait(
                    message_tasks, return_when=asyncio.FIRST_COMPLETED
                )
                await process_message_job_results(done)

            message_tasks.add(
                asyncio.create_task(handle_pending_message(pending, seen_ids))
            )

        # Wait for the last tasks
        done, _ = await asyncio.wait(
            message_tasks, return_when=asyncio.ALL_COMPLETED
        )
        await process_message_job_results(done)

        # TODO: move this to a dedicated job and/or check unicity on insertion
        #       in pending messages
        if await PendingMessage.collection.count_documents(find_params) > 100000:
            LOGGER.info("Cleaning messages")
            clean_actions = []
            # big collection, try to remove dups.
            for key, height in seen_ids.items():
                clean_actions.append(
                    DeleteMany(
                        {
                            "message.item_hash": key[0],
                            "message.sender": key[1],
                            "source.chain_name": key[2],
                            "source.height": {"$gt": height},
                        }
                    )
                )
            result = await PendingMessage.collection.bulk_write(clean_actions)
            LOGGER.info(repr(result))


async def retry_messages_task(shared_stats: Dict):
    """Handle message that were added to the pending queue"""
    await asyncio.sleep(4)
    while True:
        try:
            await process_pending_messages(shared_stats=shared_stats)
            await asyncio.sleep(5)

        except Exception:
            LOGGER.exception("Error in pending messages retry job")

        LOGGER.debug("Waiting 5 seconds for new pending messages...")
        await asyncio.sleep(5)


def pending_messages_subprocess(
    config_values: Dict, shared_stats: Dict, api_servers: List
):
    """
    Background task that processes all the messages received by the node.

    :param config_values: Application configuration, as a dictionary.
    :param shared_stats: Dictionary of application metrics. This dictionary is updated by othe
                         processes and must be allocated from shared memory.
    :param api_servers: List of Core Channel Nodes with an HTTP interface found on the network.
                        This list is updated by other processes and must be allocated from
                        shared memory by the caller.
    """

    setproctitle("aleph.jobs.messages_task_loop")
    loop, config = prepare_loop(config_values)

    sentry_sdk.init(
        dsn=config.sentry.dsn.value,
        traces_sample_rate=config.sentry.traces_sample_rate.value,
        ignore_errors=[KeyboardInterrupt],
    )
    setup_logging(
        loglevel=config.logging.level.value,
        filename="/tmp/messages_task_loop.log",
        max_log_file_size=config.logging.max_log_file_size.value,
    )
    singleton.api_servers = api_servers

    loop.run_until_complete(retry_messages_task(shared_stats))
