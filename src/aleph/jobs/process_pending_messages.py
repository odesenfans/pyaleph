"""
Job in charge of (re-) processing Aleph messages waiting in the pending queue.
"""

import asyncio
from collections import defaultdict
from logging import getLogger
from typing import Any, Dict, List, Set, Tuple, cast, Optional, AsyncIterator, Sequence

import sentry_sdk
from aleph_message.models import MessageType
from configmanager import Config, NotFound
from setproctitle import setproctitle
from sqlalchemy import update

from aleph.chains.chain_service import ChainService
from aleph.db.accessors.messages import (
    make_message_status_upsert_query,
    reject_message,
    reject_pending_message,
)
from aleph.db.accessors.pending_messages import (
    get_pending_messages,
    increase_pending_message_retry_count,
    delete_pending_message,
)
from aleph.db.connection import make_engine, make_session_factory
from aleph.db.models import PendingMessageDb, MessageDb, MessageStatusDb
from aleph.handlers.message_handler import MessageHandler
from aleph.logging import setup_logging
from aleph.services.ipfs import IpfsService
from aleph.services.ipfs.common import make_ipfs_client
from aleph.services.p2p import singleton
from aleph.services.storage.fileystem_engine import FileSystemStorageEngine
from aleph.storage import StorageService
from aleph.types.db_session import DbSession, DbSessionFactory
from aleph.types.message_status import (
    MessageUnavailable,
    InvalidMessageException,
    MessageStatus,
    PermissionDenied,
)
from .job_utils import prepare_loop

LOGGER = getLogger("jobs.pending_messages")


def _init_semaphores(config: Config) -> Dict[MessageType, asyncio.BoundedSemaphore]:
    semaphores = {}
    config_section = config.aleph.jobs.pending_messages
    max_concurrency = config_section.max_concurrency.value

    for message_type in MessageType:
        try:
            sem_value = getattr(config_section, message_type.lower()).value
        except NotFound:
            sem_value = max_concurrency

        LOGGER.debug("%s: sem_value=%d", message_type, sem_value)
        semaphores[cast(MessageType, message_type)] = asyncio.BoundedSemaphore(
            sem_value
        )

    return semaphores


ProcessingMessageId = Tuple[str, str, Optional[str], Optional[int]]


def _get_pending_message_id(pending_message: PendingMessageDb) -> ProcessingMessageId:
    source = pending_message.tx

    if source:
        chain_name, height = source.chain.value, source.height
    else:
        chain_name, height = None, None

    return (
        pending_message.item_hash,
        pending_message.sender,
        chain_name,
        height,
    )


async def _finalize_pending_message(
    session: DbSession, pending_message: PendingMessageDb, new_status: MessageStatus
):
    # We need to use an upsert here because another concurrent task could
    # change the status of the message. Upserting guarantees that the message
    # status will only be changed if the message is still marked as pending.
    session.execute(
        make_message_status_upsert_query(
            item_hash=pending_message.item_hash,
            new_status=new_status,
            where=(MessageStatusDb.status == MessageStatus.PENDING),
        )
    )
    await delete_pending_message(session=session, pending_message=pending_message)


# TODO: use update instead, upsert makes no sense for fetched messages
async def _finalize_fetched_message(
    session: DbSession, message: MessageDb, new_status: MessageStatus
):
    session.execute(
        make_message_status_upsert_query(
            item_hash=message.item_hash,
            new_status=new_status,
            where=(MessageStatusDb.status == MessageStatus.FETCHED),
        )
    )


async def _reject_pending_message(
    session: DbSession,
    pending_message: PendingMessageDb,
    exception: InvalidMessageException,
):
    await _finalize_pending_message(
        session=session,
        pending_message=pending_message,
        new_status=MessageStatus.REJECTED,
    )


async def _reject_fetched_message(session: DbSession, message: MessageDb):
    await _finalize_fetched_message(
        session=session, message=message, new_status=MessageStatus.REJECTED
    )


async def _mark_pending_message_as_fetched(
    session: DbSession, pending_message: PendingMessageDb
):
    await _finalize_pending_message(
        session=session,
        pending_message=pending_message,
        new_status=MessageStatus.FETCHED,
    )


async def _mark_fetched_message_as_processed(session: DbSession, message: MessageDb):
    await _finalize_fetched_message(
        session=session,
        message=message,
        new_status=MessageStatus.PROCESSED,
    )


async def handle_fetch_error(
    session: DbSession, pending_message: PendingMessageDb, exception: BaseException
):
    if isinstance(exception, InvalidMessageException):
        LOGGER.warning(
            "Rejecting invalid pending message: %s", pending_message.item_hash
        )
        await reject_pending_message(
            session=session, pending_message=pending_message, exception=exception
        )
    else:
        if isinstance(exception, MessageUnavailable):
            LOGGER.warning("Could not fetch message, retrying later")
        else:
            LOGGER.exception("Unexpected error while fetching message")
        await increase_pending_message_retry_count(
            session=session, pending_message=pending_message
        )


class PendingMessageProcessor:
    def __init__(
        self, session_factory: DbSessionFactory, message_handler: MessageHandler
    ):
        self.session_factory = session_factory
        self.message_handler = message_handler

    async def _fetch_pending_message(
        self,
        session: DbSession,
        pending_message: PendingMessageDb,
        sem: asyncio.Semaphore,
    ) -> Optional[MessageDb]:
        """
        Fetches the content and related data of an Aleph message.

        Aleph messages can be incomplete when received from the network. This task fetches
        the content of the message itself if the message item type != inline and then
        fetches any related data (ex: the file targeted by a STORE message).

        At the end, the task marks the message as fetched or discards it/marks it for retry.

        :param pending_message: Pending message to fetch.
        :param sem: The semaphore that limits the number of concurrent operations.
        :return:
        """

        async with sem:
            return await self.message_handler.verify_and_fetch(
                session=session, pending_message=pending_message
            )

    async def _handle_fetch_results(
        self,
        session: DbSession,
        finished_tasks: Set[asyncio.Task],
        task_message_dict: Dict[asyncio.Task, PendingMessageDb],
        shared_stats: Dict[str, Any],
        processing_messages: Set[ProcessingMessageId],
    ) -> List[MessageDb]:
        fetched_messages = []

        for finished_task in finished_tasks:
            pending_message = task_message_dict.pop(finished_task)

            if exception := finished_task.exception():
                await handle_fetch_error(
                    session=session,
                    pending_message=pending_message,
                    exception=exception,
                )
            else:
                message = finished_task.result()
                # Confirmations return None and can be discarded here
                if message:
                    fetched_messages.append(message)
                await _mark_pending_message_as_fetched(
                    session=session, pending_message=pending_message
                )

            message_type = pending_message.type
            shared_stats["retry_messages_job_tasks"] -= 1
            shared_stats["message_jobs"][message_type] -= 1

            pending_message_id = _get_pending_message_id(pending_message)
            processing_messages.remove(pending_message_id)

        return fetched_messages

    async def fetch_pending_messages(
        self, session: DbSession, config: Config, shared_stats: Dict, loop: bool = True
    ) -> AsyncIterator[Sequence[MessageDb]]:

        processing_messages: Set[ProcessingMessageId] = set()

        max_concurrent_tasks = config.aleph.jobs.pending_messages.max_concurrency.value
        semaphores = _init_semaphores(config)

        # Reset stats to avoid nonsensical values if the job restarts
        shared_stats["retry_messages_job_tasks"] = 0
        for message_type in MessageType:
            shared_stats["message_jobs"][message_type] = 0

        # Using a set is required as asyncio.wait takes and returns sets.
        fetch_tasks: Set[asyncio.Task] = set()
        task_message_dict: Dict[asyncio.Task, PendingMessageDb] = {}
        fetched_messages = []

        while True:
            # 1. Wait for existing fetch tasks to complete
            # 2. Add new tasks until the maximum amount is reached to maximize throughput
            # 3. Yield fetched messages to the next pipeline step
            if fetch_tasks:
                finished_tasks, fetch_tasks = await asyncio.wait(
                    fetch_tasks, return_when=asyncio.FIRST_COMPLETED
                )
                fetched_messages = await self._handle_fetch_results(
                    session=session,
                    finished_tasks=finished_tasks,
                    task_message_dict=task_message_dict,
                    shared_stats=shared_stats,
                    processing_messages=processing_messages,
                )
                session.commit()

            if len(fetch_tasks) < max_concurrent_tasks:
                pending_messages = list(
                    await get_pending_messages(
                        session=session, limit=max_concurrent_tasks - len(fetch_tasks)
                    )
                )

                s = set(pm.item_hash for pm in pending_messages)
                LOGGER.warning(
                    "Pending: %d unique, %d total", len(s), len(pending_messages)
                )
                LOGGER.warning("Processing: %d", len(processing_messages))

                for pending_message in pending_messages:
                    # Check if the message is already processing
                    pending_message_id = _get_pending_message_id(pending_message)
                    if pending_message_id in processing_messages:
                        # Skip the message, we're already processing it
                        continue
                    processing_messages.add(pending_message_id)

                    message_type = pending_message.type

                    shared_stats["retry_messages_job_tasks"] += 1
                    shared_stats["message_jobs"][message_type] += 1

                    message_task = asyncio.create_task(
                        self._fetch_pending_message(
                            session=session,
                            pending_message=pending_message,
                            sem=semaphores[message_type],
                        )
                    )
                    fetch_tasks.add(message_task)
                    task_message_dict[message_task] = pending_message

            if fetched_messages:
                yield fetched_messages
                fetched_messages = []

            if not await PendingMessageDb.count(session):
                # If not in loop mode, stop if there are no more pending messages
                if not loop:
                    break
                # If we are done, wait a few seconds until retrying
                if not fetch_tasks:
                    await asyncio.sleep(5)

    async def check_permissions(
        self, session: DbSession, message_iterator: AsyncIterator[Sequence[MessageDb]]
    ) -> AsyncIterator[Sequence[MessageDb]]:
        async for fetched_messages in message_iterator:
            checked_messages = []
            for fetched_message in fetched_messages:
                try:
                    await self.message_handler.check_permissions(
                        session=session, message=fetched_message
                    )
                    checked_messages.append(fetched_message)
                except PermissionDenied as e:
                    await reject_message(
                        session=session,
                        item_hash=fetched_message.item_hash,
                        exception=e,
                    )

            session.commit()
            yield checked_messages

    async def process_messages(
        self, session: DbSession, message_iterator: AsyncIterator[Sequence[MessageDb]]
    ) -> AsyncIterator[Sequence[MessageDb]]:

        async for messages in message_iterator:
            await self.message_handler.process(session=session, messages=messages)
            for message in messages:
                # TODO: move to accessor
                session.execute(
                    update(MessageStatusDb)
                    .values(status=MessageStatus.PROCESSED)
                    .where(MessageStatusDb.item_hash == message.item_hash)
                )

            session.commit()
            yield messages

    def make_pipeline(
        self, session: DbSession, config: Config, shared_stats: Dict, loop: bool = True
    ) -> AsyncIterator[Sequence[MessageDb]]:
        fetch_iterator = self.fetch_pending_messages(
            session=session, config=config, shared_stats=shared_stats, loop=loop
        )
        permission_check_iterator = self.check_permissions(
            session=session, message_iterator=fetch_iterator
        )
        process_iterator = self.process_messages(
            session=session, message_iterator=permission_check_iterator
        )
        return process_iterator


async def fetch_and_process_messages_task(config: Config, shared_stats: Dict):
    # TODO: this sleep can probably be removed
    await asyncio.sleep(4)

    engine = make_engine(config)
    session_factory = make_session_factory(engine)

    ipfs_client = make_ipfs_client(config)
    ipfs_service = IpfsService(ipfs_client=ipfs_client)
    storage_service = StorageService(
        storage_engine=FileSystemStorageEngine(folder=config.storage.folder.value),
        ipfs_service=ipfs_service,
    )
    chain_service = ChainService(
        session_factory=session_factory, storage_service=storage_service
    )
    message_handler = MessageHandler(
        session_factory=session_factory,
        chain_service=chain_service,
        storage_service=storage_service,
    )
    pending_message_processor = PendingMessageProcessor(
        session_factory=session_factory, message_handler=message_handler
    )

    while True:
        with session_factory() as session:
            try:
                message_processing_pipeline = pending_message_processor.make_pipeline(
                    session=session, config=config, shared_stats=shared_stats
                )
                async for processed_messages in message_processing_pipeline:
                    for processed_message in processed_messages:
                        LOGGER.info(
                            "Successfully processed %s", processed_message.item_hash
                        )

            except Exception as e:
                print(e)
                LOGGER.exception("Error in pending messages job")
                session.rollback()

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

    loop.run_until_complete(
        fetch_and_process_messages_task(config=config, shared_stats=shared_stats)
    )
