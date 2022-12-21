from __future__ import annotations

import logging
from typing import List, Tuple, cast, Sequence

from aioipfs.api import RepoAPI
from aioipfs.exceptions import NotPinnedError
from aleph_message.models import (
    ItemType,
    MessageType,
    ForgetContent,
    StoreContent,
    AggregateContent,
    ItemHash,
)
from sqlalchemy import select

from aleph.db.accessors.aggregates import (
    aggregate_exists,
    delete_aggregate_element,
    refresh_aggregate,
)
from aleph.db.accessors.files import (
    is_pinned_file,
    delete_file_pin,
    delete_file as delete_file_db,
    refresh_file_tag,
)
from aleph.db.accessors.messages import (
    message_exists,
    get_message_status,
    append_to_forgotten_by,
    forget_message,
    get_message_by_item_hash,
)
from aleph.db.accessors.posts import delete_post
from aleph.db.models import MessageDb, AggregateElementDb
from aleph.handlers.content.content_handler import ContentHandler
from aleph.handlers.content.store import make_file_tag
from aleph.storage import StorageService
from aleph.types.db_session import DbSessionFactory, DbSession
from aleph.types.message_status import (
    InvalidMessageException,
    MessageUnavailable,
    MessageStatus,
    PermissionDenied,
    MissingDependency,
)
from aleph.utils import item_type_from_hash

logger = logging.getLogger(__name__)


class ForgetMessageHandler(ContentHandler):
    def __init__(
        self, session_factory: DbSessionFactory, storage_service: StorageService
    ):
        self.session_factory = session_factory
        self.storage_service = storage_service

    async def check_dependencies(self, session: DbSession, message: MessageDb) -> None:
        """
        We only consider FORGETs as fetched if the messages / aggregates they target
        already exist. Otherwise, we retry them later.
        """

        content = message.parsed_content
        message_item_hash = message.item_hash

        logger.debug(f"{message_item_hash}: checking for forget message targets")
        assert isinstance(content, ForgetContent)

        if not content.hashes and not content.aggregates:
            # The user did not specify anything to forget.
            raise InvalidMessageException(
                f"FORGET message {message_item_hash} specifies nothing to forget"
            )

        for item_hash in content.hashes:
            if not await message_exists(session=session, item_hash=item_hash):
                raise MissingDependency(
                    f"A target of FORGET message {message_item_hash} "
                    f"is not yet available: {item_hash}"
                )

        for aggregate_key in content.aggregates:
            if not aggregate_exists(
                session=session, key=aggregate_key, owner=content.address
            ):
                raise MissingDependency(
                    f"An aggregate listed in FORGET message {message_item_hash} "
                    f"is not yet available: {content.address}/{aggregate_key}"
                )

    @staticmethod
    async def delete_aggregate_element(
        session: DbSession, aggregate_message: MessageDb
    ):
        content = cast(AggregateContent, aggregate_message.parsed_content)
        logger.debug("Deleting aggregate element %s...", aggregate_message.item_hash)
        await delete_aggregate_element(
            session=session, item_hash=aggregate_message.item_hash
        )
        logger.debug("Refreshing aggregate %s/%s...", content.address, content.key)
        await refresh_aggregate(session=session, owner=content.address, key=content.key)

    @staticmethod
    async def delete_post(session: DbSession, post_message: MessageDb):
        logger.debug("Deleting post %s...", post_message.item_hash)
        await delete_post(session=session, item_hash=post_message.item_hash)

    async def delete_store(self, session: DbSession, store_message: MessageDb):
        store_content = cast(StoreContent, store_message.parsed_content)
        await delete_file_pin(session=session, item_hash=store_message.item_hash)
        await refresh_file_tag(
            session=session,
            tag=make_file_tag(
                owner=store_content.address,
                ref=store_content.ref,
                item_hash=store_message.item_hash,
            ),
        )
        await self.garbage_collect(
            session=session,
            storage_hash=store_content.item_hash,
            storage_type=store_content.item_type,
        )

    async def garbage_collect(
        self, session: DbSession, storage_hash: str, storage_type: ItemType
    ):
        """If a file does not have any reference left, delete or unpin it.

        This is typically called after 'forgetting' a message.
        """
        logger.debug(f"Garbage collecting {storage_hash}")

        if await is_pinned_file(session=session, file_hash=storage_hash):
            logger.debug(f"File {storage_hash} has at least one reference left")
            return

        # Unpin the file from IPFS or remove it from local storage
        storage_detected: ItemType = item_type_from_hash(storage_hash)

        if storage_type != storage_detected:
            raise ValueError(
                f"Inconsistent ItemType {storage_type} != {storage_detected} "
                f"for hash '{storage_hash}'"
            )

        await delete_file_db(session=session, file_hash=storage_hash)

        if storage_type == ItemType.ipfs:
            logger.debug(f"Removing from IPFS: {storage_hash}")
            ipfs_client = self.storage_service.ipfs_service.ipfs_client
            try:
                result = await ipfs_client.pin.rm(storage_hash)
                print(result)

                # Launch the IPFS garbage collector (`ipfs repo gc`)
                async for _ in RepoAPI(driver=ipfs_client).gc():
                    pass

            except NotPinnedError:
                logger.debug("File not pinned")

            logger.debug(f"Removed from IPFS: {storage_hash}")
        elif storage_type == ItemType.storage:
            logger.debug(f"Removing from local storage: {storage_hash}")
            await self.storage_service.storage_engine.delete(storage_hash)
            logger.debug(f"Removed from local storage: {storage_hash}")
        else:
            raise ValueError(f"Invalid storage type {storage_type}")
        logger.debug(f"Removed from {storage_type}: {storage_hash}")

    @staticmethod
    async def _list_target_messages(
        session: DbSession, forget_message: MessageDb
    ) -> Sequence[ItemHash]:
        content = cast(ForgetContent, forget_message.parsed_content)

        aggregate_messages_to_forget: List[ItemHash] = []
        for aggregate in content.aggregates:
            # TODO: write accessor
            aggregate_messages_to_forget.extend(
                ItemHash(value)
                for value in session.execute(
                    select(AggregateElementDb.item_hash).where(
                        (AggregateElementDb.key == aggregate)
                        & (AggregateElementDb.owner == content.address)
                    )
                ).scalars()
            )

        return content.hashes + aggregate_messages_to_forget

    async def check_permissions(self, session: DbSession, message: MessageDb):
        await super().check_permissions(session=session, message=message)

        # Check that the sender owns the objects it is attempting to forget
        target_hashes = await self._list_target_messages(
            session=session, forget_message=message
        )
        for target_hash in target_hashes:
            target_status = await get_message_status(
                session=session, item_hash=target_hash
            )
            if not target_status:
                raise MessageUnavailable(
                    f"Target message {target_hash} is not known by this node."
                )

            if target_status.status in (
                MessageStatus.FORGOTTEN,
                MessageStatus.REJECTED,
            ):
                continue

            if target_status.status != MessageStatus.PROCESSED:
                raise MessageUnavailable(
                    f"Target message {target_hash} is not yet processed."
                )

            target_message = await get_message_by_item_hash(
                session=session, item_hash=target_hash
            )
            if not target_message:
                raise MessageUnavailable(
                    f"Target message {target_hash} is marked as processed but does not exist."
                )
            if target_message.type == MessageType.forget:
                raise PermissionDenied(
                    f"FORGET message {target_hash} may not be forgotten by {message.item_hash}"
                )
            if target_message.sender != message.sender:
                raise PermissionDenied(
                    f"Cannot forget message {target_hash} because it belongs to another user"
                )

    async def _forget_by_message_type(self, session: DbSession, message: MessageDb):
        """
        When processing a FORGET message, performs additional cleanup depending
        on the type of message that is being forgotten.
        """

        if message.type == MessageType.aggregate:
            await self.delete_aggregate_element(
                session=session, aggregate_message=message
            )
        elif message.type == MessageType.post:
            await self.delete_post(session=session, post_message=message)
        elif message.type == MessageType.store:
            await self.delete_store(session=session, store_message=message)

    async def _forget_message(
        self, session: DbSession, message: MessageDb, forgotten_by: MessageDb
    ):
        # Mark the message as forgotten
        await forget_message(
            session=session,
            item_hash=message.item_hash,
            forget_message_hash=forgotten_by.item_hash,
        )

        await self._forget_by_message_type(session=session, message=message)

    async def _forget_item_hash(
        self, session: DbSession, item_hash: str, forgotten_by: MessageDb
    ):
        message_status = await get_message_status(session=session, item_hash=item_hash)
        if not message_status:
            raise MessageUnavailable(
                f"Target message {item_hash} is not known by this node."
            )

        if message_status.status == MessageStatus.REJECTED:
            logger.info("Message %s was rejected, nothing to do.", item_hash)
        if message_status.status == MessageStatus.FORGOTTEN:
            logger.info("Message %s is already forgotten, nothing to do.", item_hash)
            await append_to_forgotten_by(
                session=session,
                forgotten_message_hash=item_hash,
                forget_message_hash=forgotten_by.item_hash,
            )
            return

        if message_status.status != MessageStatus.PROCESSED:
            logger.error(
                "FORGET message %s targets message %s which is not processed yet. This should not happen.",
                forgotten_by.item_hash,
                item_hash,
            )
            raise MessageUnavailable(
                "Trying to process a FORGET message with target messages not processed!"
            )

        message = await get_message_by_item_hash(session=session, item_hash=item_hash)
        if not message:
            raise MessageUnavailable(f"Message {item_hash} not found")

        if message.type == MessageType.forget:
            # This should have been detected in check_permissions(). Raise an exception
            # if it happens nonetheless as it indicates an unforeseen concurrent modification
            # of the database.
            raise PermissionDenied("Cannot forget a FORGET message")

        await self._forget_message(
            session=session,
            message=message,
            forgotten_by=forgotten_by,
        )

    async def _process_forget_message(self, session: DbSession, message: MessageDb):

        hashes_to_forget = await self._list_target_messages(
            session=session, forget_message=message
        )

        for item_hash in hashes_to_forget:
            await self._forget_item_hash(
                session=session, item_hash=item_hash, forgotten_by=message
            )

    async def process(
        self, session: DbSession, messages: List[MessageDb]
    ) -> Tuple[List[MessageDb], List[MessageDb]]:

        # FORGET:
        # 0. Check permissions: separate step now
        # 1. Check if the message is already forgotten -> if yes, add to forgotten_by and done
        # 2. Get all the messages to forget, including aggregates
        # 3. Forget the messages
        # 4. For each type of message, perform an additional check

        for message in messages:
            await self._process_forget_message(session=session, message=message)

        return messages, []
