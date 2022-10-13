from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional, List, Mapping, Tuple

from aioipfs.api import RepoAPI
from aioipfs.exceptions import NotPinnedError
from aleph_message.models import ItemType, MessageType, ForgetContent
from sqlalchemy import update

from aleph.db.accessors.aggregates import (
    aggregate_exists,
    delete_aggregate,
    get_message_hashes_for_aggregate,
)
from aleph.db.accessors.files import is_pinned_file
from aleph.db.accessors.messages import message_exists, get_matching_messages
from aleph.db.models import MessageDb, MessageStatusDb
from aleph.handlers.content.content_handler import ContentHandler
from aleph.model.messages import Message
from aleph.schemas.validated_message import ValidatedForgetMessage
from aleph.storage import StorageService
from aleph.types.db_session import DbSessionFactory, DbSession
from aleph.types.message_status import (
    MessageProcessingStatus,
    InvalidMessage,
    MessageUnavailable,
    MessageStatus,
)
from aleph.utils import item_type_from_hash

logger = logging.getLogger(__name__)


@dataclass
class TargetMessageInfo:
    item_hash: str
    sender: str
    type: MessageType
    forgotten_by: List[str]
    content_address: Optional[str]
    content_item_hash: Optional[str]
    content_item_type: Optional[ItemType]

    @classmethod
    def from_db_object(cls, message_dict: Mapping) -> TargetMessageInfo:
        content = message_dict.get("", {})
        content_item_type = content.get("item_type")

        if content_item_type is not None:
            content_item_type = ItemType(content_item_type)

        return cls(
            item_hash=message_dict["item_hash"],
            sender=message_dict["sender"],
            type=MessageType(message_dict["type"]),
            forgotten_by=message_dict.get("forgotten_by", []),
            content_address=content.get("address"),
            content_item_hash=content.get("item_hash"),
            content_item_type=content_item_type,
        )


async def get_target_message_info(target_hash: str) -> Optional[TargetMessageInfo]:
    message_dict = await Message.collection.find_one(
        filter={"item_hash": target_hash},
        projection={
            "_id": 0,
            "item_hash": 1,
            "sender": 1,
            "type": 1,
            "forgotten_by": 1,
            "content.address": 1,
            "content.item_hash": 1,
            "content.item_type": 1,
        },
    )
    if message_dict is None:
        return None

    return TargetMessageInfo.from_db_object(message_dict)


async def count_file_references(storage_hash: str) -> int:
    """Count the number of references to a file on Aleph."""
    logger.debug(f"Counting documents for {storage_hash}")
    return await Message.collection.count_documents(
        filter={"content.item_hash": storage_hash},
    )


async def file_references_exist(storage_hash: str) -> bool:
    """Check if references to a file on Aleph exist."""
    return bool(
        await Message.collection.count_documents(
            filter={"content.item_hash": storage_hash}, limit=1
        )
    )


class ForgetMessageHandler(ContentHandler):
    def __init__(
        self, session_factory: DbSessionFactory, storage_service: StorageService
    ):
        self.session_factory = session_factory
        self.storage_service = storage_service

    async def fetch_related_content(
        self, session: DbSession, message: MessageDb
    ) -> None:
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
            raise InvalidMessage(
                f"FORGET message {message_item_hash} specifies nothing to forget"
            )

        for item_hash in content.hashes:
            if not message_exists(session=session, item_hash=item_hash):
                raise MessageUnavailable(
                    f"A target of FORGET message {message_item_hash} "
                    f"is not yet available: {item_hash}"
                )

        for aggregate_key in content.aggregates:
            if not aggregate_exists(
                session=session, key=aggregate_key, owner=content.address
            ):
                raise MessageUnavailable(
                    f"An aggregate listed in FORGET message {message_item_hash} "
                    f"is not yet available: {content.address}/{aggregate_key}"
                )

    async def garbage_collect(self, storage_hash: str, storage_type: ItemType):
        """If a file does not have any reference left, delete or unpin it.

        This is typically called after 'forgetting' a message.
        """
        logger.debug(f"Garbage collecting {storage_hash}")

        with self.session_factory() as session:
            if await is_pinned_file(session=session, file_hash=storage_hash):
                logger.debug(f"Permanent pin will not be collected {storage_hash}")
                return

        if await file_references_exist(storage_hash):
            logger.debug(f"File {storage_hash} has at least one reference left")
            return

        # Unpin the file from IPFS or remove it from local storage
        storage_detected: ItemType = item_type_from_hash(storage_hash)

        if storage_type != storage_detected:
            raise ValueError(
                f"Inconsistent ItemType {storage_type} != {storage_detected} "
                f"for hash '{storage_hash}'"
            )

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
    async def is_allowed_to_forget(
        target_info: TargetMessageInfo, by: ValidatedForgetMessage
    ) -> bool:
        """Check if a forget message is allowed to 'forget' the target message given its hash."""
        # Both senders are identical:
        if by.sender == target_info.sender:
            return True
        else:
            # Content already forgotten, probably by someone else
            if target_info.content_address is None:
                return False

            # The forget sender matches the content address:
            if by.sender == target_info.content_address:
                return True
        return False

    async def forget_if_allowed(
        self, target_info: TargetMessageInfo, forget_message: ValidatedForgetMessage
    ) -> None:
        """Forget a message.

        Remove the ‘content’ and ‘item_content’ sections of the targeted messages.
        Add a field ‘removed_by’ that references to the processed FORGET message.
        """
        target_hash = target_info.item_hash

        if target_info.type == MessageType.forget:
            logger.info(
                f"FORGET message may not be forgotten {target_hash} by {forget_message.item_hash}"
            )
            return

        # TODO: support forgetting the same message several times (if useful)
        if target_info.forgotten_by:
            logger.debug(f"Message content already forgotten: {target_hash}")
            return

        if not await self.is_allowed_to_forget(target_info, by=forget_message):
            logger.info(
                f"Not allowed to forget {target_hash} by {forget_message.item_hash}"
            )
            return

        logger.debug(f"Removing content for {target_hash}")
        updates = {
            "content": None,
            "item_content": None,
            "forgotten_by": [forget_message.item_hash],
        }
        await Message.collection.update_many(
            filter={"item_hash": target_hash}, update={"$set": updates}
        )

        # TODO QUESTION: Should the removal be added to the CappedMessage collection for websocket
        #  updates ? Forget messages should already be published there, but the logic to validate
        #  them could be centralized here.

        if target_info.type == MessageType.store:
            if (
                target_info.content_item_type is None
                or target_info.content_item_hash is None
            ):
                raise ValueError(
                    f"Could not garbage collect content linked to STORE message {target_hash}."
                )

            await self.garbage_collect(
                target_info.content_item_hash, target_info.content_item_type
            )

    async def _forget_aggregate(self, session: DbSession, key: str, owner: str):
        await delete_aggregate(session=session, key=key, owner=owner)

    async def _process_forget_message(
        self, session: DbSession, forget_message: MessageDb
    ) -> MessageProcessingStatus:

        logger.debug(f"Processing forget message {forget_message.item_hash}...")

        content = forget_message.parsed_content
        assert isinstance(content, ForgetContent)

        messages_to_forget = content.hashes
        for aggregate_key in content.aggregates:
            messages_to_forget.extend(
                await get_message_hashes_for_aggregate(
                    session=session, key=aggregate_key, owner=content.address
                )
            )

        if content.hashes:
            target_messages = {
                message.item_hash: message
                for message in await get_matching_messages(
                    session=session, hashes=content.hashes
                )
            }
        else:
            target_messages = []

        await self._check_permissions()

        if content.aggregates:
            # Check permissions: for the moment, we only allow users to forget messages
            # directly
            if forget_message.sender != content.address:
                return MessageProcessingStatus.FAILED_PERMANENTLY

        for aggregate_key in content.aggregates:
            await self._forget_aggregate(
                session=session, key=aggregate_key, owner=content.address
            )

        for target_hash in hashes_to_forget:
            target_info = await get_target_message_info(target_hash)

            if target_info is None:
                logger.info(
                    f"Message to forget could not be found with id {target_hash}"
                )
                continue

            await self.forget_if_allowed(
                target_info=target_info, forget_message=forget_message
            )

    async def check_permissions(self, session: DbSession, message: MessageDb):
        await super().check_permissions(session=session, message=message)

        # Check that the sender owns the objects it is attempting to forget

    async def _forget_message(self, session: DbSession, item_hash: str):
        session.execute(
            update(MessageStatusDb)
            .where((MessageStatusDb.item_hash == item_hash) & (MessageStatusDb.status == MessageStatus.PROCESSED))
            .values(status=MessageStatus.FORGOTTEN)
        )

    async def _process_forget_message(
        self, session: DbSession, forget_message: MessageDb
    ):
        await self.check_permissions(session=session, message=forget_message)

    async def process(
        self, session: DbSession, messages: List[MessageDb]
    ) -> Tuple[List[MessageDb], List[MessageDb]]:
        return [], []
