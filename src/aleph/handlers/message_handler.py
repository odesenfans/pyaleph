from enum import IntEnum
from typing import Optional, Dict, Tuple, List

from aleph_message.models import MessageConfirmation
from bson import ObjectId
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from aleph.chains.chain_service import ChainService
from aleph.chains.common import LOGGER
from aleph.db.accessors.messages import (
    make_message_upsert_query,
    make_confirmation_upsert_query,
    get_message_by_item_hash,
)
from aleph.db.bulk_operations import DbBulkOperation
from aleph.db.models import PendingMessageDb, MessageDb, MessageConfirmationDb
from aleph.exceptions import (
    InvalidMessageError,
    InvalidContent,
    ContentCurrentlyUnavailable,
    UnknownHashError,
)
from aleph.permissions import check_sender_authorization
from aleph.schemas.pending_messages import BasePendingMessage
from aleph.schemas.validated_message import (
    validate_pending_message,
    ValidatedStoreMessage,
    ValidatedForgetMessage,
)
from aleph.storage import StorageService
from .forget import ForgetMessageHandler
from .storage import StoreMessageHandler
from aleph.types.db_session import DbSessionFactory


class IncomingStatus(IntEnum):
    FAILED_PERMANENTLY = -1
    RETRYING_LATER = 0
    MESSAGE_HANDLED = 1


class MessageHandler:
    def __init__(
        self,
        session_factory: DbSessionFactory,
        chain_service: ChainService,
        storage_service: StorageService,
    ):
        self.session_factory = session_factory
        self.chain_service = chain_service
        self.storage_service = storage_service

        self.store_message_handler = StoreMessageHandler(
            storage_service=storage_service
        )
        self.forget_message_handler = ForgetMessageHandler(
            session_factory=session_factory, storage_service=storage_service
        )

    @staticmethod
    async def _mark_message_for_retry(
        session: AsyncSession,
        message: BasePendingMessage,
        tx_hash: Optional[str],
        check_message: bool,
        retrying: bool,
        existing_id,
    ):
        if not retrying:
            session.add(
                PendingMessageDb.from_obj(
                    obj=message,
                    tx_hash=tx_hash,
                    check_message=check_message,  # should we store this?
                )
            )
        else:
            LOGGER.debug(f"Incrementing for {existing_id}")
            update_stmt = (
                update(PendingMessageDb)
                .where(PendingMessageDb.item_hash == message.item_hash)
                .values(retries=PendingMessageDb.retries + 1)
            )
            await session.execute(update_stmt)

    async def delayed_incoming(
        self,
        message: BasePendingMessage,
        tx_hash: Optional[str] = None,
    ):

        async with self.session_factory() as session:
            session.add(
                PendingMessageDb.from_obj(message, tx_hash=tx_hash, check_message=True)
            )
            await session.commit()

    async def incoming(
        self,
        pending_message: BasePendingMessage,
        chain_name: Optional[str] = None,
        tx_hash: Optional[str] = None,
        height: Optional[int] = None,
        seen_ids: Optional[Dict[Tuple, int]] = None,
        check_message: bool = False,
        retrying: bool = False,
        existing_id: Optional[ObjectId] = None,
    ) -> Tuple[IncomingStatus, List[DbBulkOperation]]:
        """New incoming message from underlying chain.

        For regular messages it will be marked as confirmed
        if existing in database, created if not.
        """

        item_hash = pending_message.item_hash
        sender = pending_message.sender
        # TODO: refactor this mechanism, no need for a list here and we can probably simplify
        #       the logic a lot
        confirmations = []
        ids_key = (item_hash, sender, chain_name)

        if chain_name and tx_hash and height:
            if seen_ids is not None:
                if ids_key in seen_ids.keys():
                    if height > seen_ids[ids_key]:
                        return IncomingStatus.MESSAGE_HANDLED, []

            confirmations.append(
                MessageConfirmation(chain=chain_name, hash=tx_hash, height=height)
            )

        async with self.session_factory() as session:
            existing = await get_message_by_item_hash(session=session, item_hash=item_hash)

        if check_message:
            if existing is None or (existing.signature != pending_message.signature):
                # check/sanitize the message if needed
                try:
                    await self.chain_service.verify_signature(pending_message)
                except InvalidMessageError:
                    return IncomingStatus.FAILED_PERMANENTLY, []

        if retrying:
            LOGGER.debug("(Re)trying %s." % item_hash)
        else:
            LOGGER.info("Incoming %s." % item_hash)

        updates = []

        if existing:
            if seen_ids is not None and height is not None:
                if ids_key in seen_ids.keys():
                    if height > seen_ids[ids_key]:
                        return IncomingStatus.MESSAGE_HANDLED, []
                    else:
                        seen_ids[ids_key] = height
                else:
                    seen_ids[ids_key] = height

            LOGGER.debug("Updating %s." % item_hash)

            if confirmations:
                updates = [
                    DbBulkOperation(
                        model=MessageConfirmationDb,
                        operation=make_confirmation_upsert_query(
                            item_hash=item_hash, tx_hash=confirmation.hash
                        ),
                    )
                    for confirmation in confirmations
                ]

        else:
            try:
                content = await self.storage_service.get_message_content(
                    pending_message
                )

            except InvalidContent:
                LOGGER.warning(
                    "Can't get content of object %r, won't retry." % item_hash
                )
                return IncomingStatus.FAILED_PERMANENTLY, []

            except (ContentCurrentlyUnavailable, Exception) as e:
                if not isinstance(e, ContentCurrentlyUnavailable):
                    LOGGER.exception("Can't get content of object %r" % item_hash)
                async with self.session_factory() as session:
                    await self._mark_message_for_retry(
                        session=session,
                        message=pending_message,
                        tx_hash=tx_hash,
                        check_message=check_message,
                        retrying=retrying,
                        existing_id=existing_id,
                    )
                return IncomingStatus.RETRYING_LATER, []

            validated_message = validate_pending_message(
                pending_message=pending_message,
                content=content,
                confirmations=confirmations,
            )

            # warning: those handlers can modify message and content in place
            # and return a status. None has to be retried, -1 is discarded, True is
            # handled and kept.
            # TODO: change this, it's messy.
            try:
                if isinstance(validated_message, ValidatedStoreMessage):
                    handling_result = (
                        await self.store_message_handler.handle_new_storage(
                            validated_message
                        )
                    )
                elif isinstance(validated_message, ValidatedForgetMessage):
                    # Handling it here means that there we ensure that the message
                    # has been forgotten before it is saved on the node.
                    # We may want the opposite instead: ensure that the message has
                    # been saved before it is forgotten.
                    handling_result = (
                        await self.forget_message_handler.handle_forget_message(
                            validated_message
                        )
                    )
                else:
                    handling_result = True
            except UnknownHashError:
                LOGGER.warning(
                    f"Invalid IPFS hash for message {item_hash}, won't retry."
                )
                return IncomingStatus.FAILED_PERMANENTLY, []
            except Exception:
                LOGGER.exception("Error using the message type handler")
                handling_result = None

            if handling_result is None:
                LOGGER.debug("Message type handler has failed, retrying later.")
                async with self.session_factory() as session:
                    await self._mark_message_for_retry(
                        session=session,
                        message=pending_message,
                        tx_hash=tx_hash,
                        check_message=check_message,
                        retrying=retrying,
                        existing_id=existing_id,
                    )
                return IncomingStatus.RETRYING_LATER, []

            if not handling_result:
                LOGGER.warning(
                    "Message type handler has failed permanently for "
                    "%r, won't retry." % item_hash
                )
                return IncomingStatus.FAILED_PERMANENTLY, []

            if not await check_sender_authorization(validated_message):
                LOGGER.warning("Invalid sender for %s" % item_hash)
                return IncomingStatus.MESSAGE_HANDLED, []

            if seen_ids is not None and height is not None:
                if ids_key in seen_ids.keys():
                    if height > seen_ids[ids_key]:
                        return IncomingStatus.MESSAGE_HANDLED, []
                    else:
                        seen_ids[ids_key] = height
                else:
                    seen_ids[ids_key] = height

            LOGGER.debug("New message to store for %s." % item_hash)

            updates = [
                DbBulkOperation(
                    model=MessageDb,
                    operation=make_message_upsert_query(validated_message),
                )
            ]
            for confirmation in validated_message.confirmations:
                updates.append(
                    DbBulkOperation(
                        model=MessageConfirmationDb,
                        operation=make_confirmation_upsert_query(
                            item_hash=validated_message.item_hash,
                            tx_hash=confirmation.hash,
                        ),
                    )
                )

        if updates:
            bulk_ops = updates

            # Capped collections do not accept updates that increase the size, so
            # we must ignore confirmations. We also ignore on-chain messages for
            # performance reasons (bulk inserts on capped collections are slow).
            # TODO: determine what to do for the websocket (ex: use the message table directly?)
            # if existing is None:
            # if tx_hash is None:
            # bulk_ops.append(DbBulkOperation(CappedMessage, update_op))

            return IncomingStatus.MESSAGE_HANDLED, bulk_ops

        return IncomingStatus.MESSAGE_HANDLED, []

    async def process_one_message(self, message: BasePendingMessage, *args, **kwargs):
        """
        Helper function to process a message on the spot.
        """

        status, ops = await self.incoming(message, *args, **kwargs)

        async with self.session_factory() as session:
            async with session.begin():
                for op in ops:
                    await session.execute(op.operation)

            await session.commit()
