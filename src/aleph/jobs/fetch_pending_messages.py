"""
Job in charge of (re-) processing Aleph messages waiting in the pending queue.
"""

import asyncio
from logging import getLogger
from typing import (
    Any,
    Dict,
    List,
    Set,
    Tuple,
    cast,
    Optional,
    AsyncIterator,
    Sequence,
    NewType,
)

import sentry_sdk
from aleph_message.models import MessageType
from configmanager import Config, NotFound
from setproctitle import setproctitle
from sqlalchemy import update

from aleph.chains.chain_service import ChainService
from aleph.db.accessors.messages import (
    reject_message,
    reject_pending_message,
)
from aleph.db.accessors.pending_messages import (
    get_pending_messages,
    increase_pending_message_retry_count,
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
from aleph.types.actions.action import ActionStatus
from aleph.types.actions.db_action import MessageDbAction, MarkPendingMessageAsFetched
from aleph.types.actions.db_executor import DbExecutor
from aleph.types.actions.executor import execute_actions

LOGGER = getLogger(__name__)


MessageId = NewType("ProcessingMessageId", str)


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


def _get_pending_message_id(pending_message: PendingMessageDb) -> MessageId:
    return MessageId(pending_message.item_hash)


async def handle_fetch_error(
    session: DbSession,
    pending_message: PendingMessageDb,
    exception: BaseException,
    max_retries: int,
):
    if isinstance(exception, InvalidMessageException):
        LOGGER.warning(
            "Rejecting invalid pending message: %s - %s",
            pending_message.item_hash,
            str(exception),
        )
        await reject_pending_message(
            session=session,
            pending_message=pending_message,
            reason=str(exception),
            exception=None,
        )
    else:
        if isinstance(exception, MessageUnavailable):
            LOGGER.warning(
                "Could not fetch message %s, retrying later: %s",
                pending_message.item_hash,
                str(exception),
            )
        else:
            LOGGER.exception(
                "Unexpected error while fetching message", exc_info=exception
            )
        if pending_message.retries >= max_retries:
            LOGGER.warning(
                "Rejecting pending message: %s - too many retries",
                pending_message.item_hash,
            )
            rejection_exception = (
                None if isinstance(exception, MessageUnavailable) else exception
            )
            await reject_pending_message(
                session=session,
                pending_message=pending_message,
                reason="Too many retries",
                exception=rejection_exception,
            )
        else:
            await increase_pending_message_retry_count(
                session=session, pending_message=pending_message
            )


class PendingMessageFetcher:
    def __init__(
        self,
        session_factory: DbSessionFactory,
        message_handler: MessageHandler,
        max_retries: int,
    ):
        self.session_factory = session_factory
        self.message_handler = message_handler
        self.max_retries = max_retries

    async def _fetch_pending_message(
        self,
        # TODO: ensure the session is read-only once storage stops inserting data
        session: DbSession,
        pending_message: PendingMessageDb,
        sem: asyncio.Semaphore,
    ) -> Tuple[MessageDb, List[MessageDbAction]]:
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
            message = await self.message_handler.verify_and_fetch_new(
                session=session, pending_message=pending_message
            )
            session.commit()
        return message, [
            MarkPendingMessageAsFetched(
                pending_message=pending_message, content=message.content
            )
        ]

    async def _handle_fetch_results(
        self,
        session: DbSession,
        finished_tasks: Set[asyncio.Task],
        task_message_dict: Dict[asyncio.Task, PendingMessageDb],
        shared_stats: Dict[str, Any],
        processing_messages: Set[MessageId],
    ) -> List[MessageDb]:

        fetched_messages = []
        actions = []

        for finished_task in finished_tasks:
            pending_message = task_message_dict.pop(finished_task)

            if exception := finished_task.exception():
                await handle_fetch_error(
                    session=session,
                    pending_message=pending_message,
                    exception=exception,
                    max_retries=self.max_retries,
                )
            else:
                message, message_actions = finished_task.result()
                actions += message_actions

                # Confirmations return None and can be discarded here
                if message:
                    fetched_messages.append(message)

            message_type = pending_message.type
            shared_stats["retry_messages_job_tasks"] -= 1
            shared_stats["message_jobs"][message_type] -= 1

            pending_message_id = _get_pending_message_id(pending_message)
            processing_messages.remove(pending_message_id)

        await execute_actions(
            actions=actions, executors=DbExecutor(self.session_factory)
        )
        for action in actions:
            if action.status == ActionStatus.FAILED:
                LOGGER.exception(
                    "Failed to process %s action for %s",
                    type(action).__name__,
                    action.pending_message.item_hash,
                    exc_info=action.error,
                )
                # TODO: find a more efficient way to remove failed messages from this list.
                #       this works for now and should be rare enough that we don't care.
                fetched_messages = [
                    message
                    for message in fetched_messages
                    if message.item_hash != action.pending_message.item_hash
                ]
                await increase_pending_message_retry_count(
                    session=session, pending_message=action.pending_message
                )

        return fetched_messages

    async def fetch_pending_messages(
        self, config: Config, shared_stats: Dict, loop: bool = True
    ):
        LOGGER.info("starting fetch job")

        processing_messages: Set[MessageId] = set()

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

        with self.session_factory() as session:
            while True:
                # 1. Wait for existing fetch tasks to complete
                # 2. Add new tasks until the maximum amount is reached to maximize throughput
                # 3. Yield fetched messages to the next pipeline step
                if fetch_tasks:
                    finished_tasks, fetch_tasks = await asyncio.wait(
                        fetch_tasks, return_when=asyncio.FIRST_COMPLETED
                    )
                    LOGGER.info(
                        "Finished fetching %d pending messages, %d still being fetched",
                        len(finished_tasks),
                        len(fetch_tasks),
                    )
                    fetched_messages = await self._handle_fetch_results(
                        session=session,
                        finished_tasks=finished_tasks,
                        task_message_dict=task_message_dict,
                        shared_stats=shared_stats,
                        processing_messages=processing_messages,
                    )
                    LOGGER.info(
                        "Successfully fetched %d messages", len(fetched_messages)
                    )
                    session.commit()

                if len(fetch_tasks) < max_concurrent_tasks:
                    pending_messages = list(
                        await get_pending_messages(
                            session=session,
                            limit=max_concurrent_tasks - len(fetch_tasks),
                            offset=len(fetch_tasks),
                            skip_store=semaphores[MessageType.store].locked(),
                            fetched=False,
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
                        LOGGER.info("waiting 5 seconds for new pending messages")
                        await asyncio.sleep(5)

    def make_pipeline(
        self,
        config: Config,
        shared_stats: Dict,
        loop: bool = True,
    ) -> AsyncIterator[Sequence[MessageDb]]:
        fetch_iterator = self.fetch_pending_messages(
            config=config, shared_stats=shared_stats, loop=loop
        )
        return fetch_iterator


async def fetch_messages_task(config: Config, shared_stats: Dict):
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
    fetcher = PendingMessageFetcher(
        session_factory=session_factory,
        message_handler=message_handler,
        max_retries=config.aleph.jobs.pending_messages.max_retries.value,
    )

    while True:
        with session_factory() as session:
            try:
                fetch_pipeline = fetcher.make_pipeline(
                    config=config, shared_stats=shared_stats
                )
                async for fetched_messages in fetch_pipeline:
                    for fetched_message in fetched_messages:
                        LOGGER.info(
                            "Successfully fetched %s", fetched_message.item_hash
                        )

            except Exception as e:
                print(e)
                LOGGER.exception("Error in pending messages job")
                session.rollback()

        LOGGER.debug("Waiting 5 seconds for new pending messages...")
        await asyncio.sleep(5)


def fetch_pending_messages_subprocess(
    config_values: Dict, shared_stats: Dict, api_servers: List
):
    """
    Background process that fetches all the messages received by the node.

    The goal of this process is to fetch all the data associated to an Aleph message, i.e.
    the content field of the message and any associated file. Furthermore, the process will
    validate that objects that the message depends on are already present in the database
    (ex: a message to forget, a post to amend, etc).

    :param config_values: Application configuration, as a dictionary.
    :param shared_stats: Dictionary of application metrics. This dictionary is updated by othe
                         processes and must be allocated from shared memory.
    :param api_servers: List of Core Channel Nodes with an HTTP interface found on the network.
                        This list is updated by other processes and must be allocated from
                        shared memory by the caller.
    """

    setproctitle("aleph.jobs.fetch_messages")
    loop, config = prepare_loop(config_values)

    sentry_sdk.init(
        dsn=config.sentry.dsn.value,
        traces_sample_rate=config.sentry.traces_sample_rate.value,
        ignore_errors=[KeyboardInterrupt],
    )
    setup_logging(
        loglevel=config.logging.level.value,
        filename="/tmp/fetch_messages.log",
        max_log_file_size=config.logging.max_log_file_size.value,
    )
    singleton.api_servers = api_servers

    asyncio.run(fetch_messages_task(config=config, shared_stats=shared_stats))
