from typing import Optional, Dict, Tuple, List

from aleph_message.models import MessageConfirmation, MessageType
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
from aleph.db.models import (
    PendingMessageDb,
    MessageDb,
    MessageConfirmationDb,
    ChainTxDb,
)
from aleph.exceptions import (
    InvalidMessageError,
    InvalidContent,
    ContentCurrentlyUnavailable,
    UnknownHashError,
)
from aleph.handlers.content.aggregate import AggregateMessageHandler
from aleph.handlers.content.content_handler import ContentHandler
from aleph.handlers.content.forget import ForgetMessageHandler
from aleph.handlers.content.storage import StoreMessageHandler
from aleph.permissions import check_sender_authorization
from aleph.schemas.pending_messages import BasePendingMessage
from aleph.storage import StorageService
from aleph.types.db_session import DbSessionFactory, DbSession
from aleph.types.message_status import MessageProcessingStatus


class MessageHandler:
    content_handlers: Dict[MessageType, ContentHandler]

    def __init__(
        self,
        session_factory: DbSessionFactory,
        chain_service: ChainService,
        storage_service: StorageService,
    ):
        self.session_factory = session_factory
        self.chain_service = chain_service
        self.storage_service = storage_service

        self.content_handlers = {
            MessageType.aggregate: AggregateMessageHandler(
                session_factory=session_factory
            ),
            MessageType.forget: ForgetMessageHandler(
                session_factory=session_factory, storage_service=storage_service
            ),
            # MessageType.post: PostMessageHandler(session_factory=session_factory),
            # MessageType.program: ProgramMessageHandler(session_factory=session_factory),
            MessageType.store: StoreMessageHandler(
                session_factory=session_factory, storage_service=storage_service
            ),
        }

    @staticmethod
    async def _mark_message_for_retry(
        session: AsyncSession,
        pending_message: PendingMessageDb,
        retrying: bool,
    ):
        if not retrying:
            session.add(pending_message)
        else:
            LOGGER.debug(
                f"Incrementing for item hash: {pending_message.item_hash} - ID: {pending_message.id}"
            )
            # TODO: write accessor instead
            update_stmt = (
                update(PendingMessageDb)
                .where(PendingMessageDb.item_hash == pending_message.item_hash)
                .values(retries=PendingMessageDb.retries + 1)
            )
            await session.execute(update_stmt)

    # TODO typing: make this function generic
    def get_content_handler(self, message_type: MessageType) -> ContentHandler:
        return self.content_handlers[message_type]

    async def process_message_content(
        self, session: DbSession, message: MessageDb
    ) -> Tuple[MessageProcessingStatus, List[DbBulkOperation]]:
        try:
            content_handler = self.content_handlers[message.message_type]
        except KeyError:
            return MessageProcessingStatus.MESSAGE_HANDLED, []

        return await content_handler.fetch_related_content(
            session=session, message=message
        )

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
        pending_message: PendingMessageDb,
        seen_ids: Optional[Dict[Tuple, int]] = None,
        retrying: bool = False,
    ) -> Tuple[MessageProcessingStatus, List[DbBulkOperation]]:
        """New incoming message from underlying chain.

        For regular messages it will be marked as confirmed
        if existing in database, created if not.
        """

        item_hash = pending_message.item_hash
        sender = pending_message.sender
        chain_tx = pending_message.tx
        # TODO: refactor this mechanism, no need for a list here and we can probably simplify
        #       the logic a lot
        confirmations: List[MessageConfirmation] = []
        if chain_tx:
            ids_key = (item_hash, sender, chain_tx.chain)
        else:
            ids_key = (item_hash, sender, None)

        if chain_tx:
            if seen_ids is not None:
                if ids_key in seen_ids.keys():
                    if chain_tx.height > seen_ids[ids_key]:
                        return MessageProcessingStatus.MESSAGE_HANDLED, []

            confirmations.append(
                MessageConfirmation(
                    chain=chain_tx.chain, hash=chain_tx.hash, height=chain_tx.height
                )
            )

        async with self.session_factory() as session:
            existing = await get_message_by_item_hash(
                session=session, item_hash=item_hash
            )

        if pending_message.check_message:
            if existing is None or (existing.signature != pending_message.signature):
                # check/sanitize the message if needed
                try:
                    # TODO: remove type: ignore by deciding the pending message type
                    await self.chain_service.verify_signature(pending_message)  # type: ignore
                except InvalidMessageError:
                    return MessageProcessingStatus.FAILED_PERMANENTLY, []

        if retrying:
            LOGGER.debug("(Re)trying %s." % item_hash)
        else:
            LOGGER.info("Incoming %s." % item_hash)

        updates = []

        if existing:
            if seen_ids is not None and chain_tx:
                if ids_key in seen_ids.keys():
                    if chain_tx.height > seen_ids[ids_key]:
                        return MessageProcessingStatus.MESSAGE_HANDLED, []
                    else:
                        seen_ids[ids_key] = chain_tx.height
                else:
                    seen_ids[ids_key] = chain_tx.height

            LOGGER.debug("Updating %s." % item_hash)

            if confirmations:
                updates = [
                    DbBulkOperation(
                        model=MessageConfirmationDb,
                        operation=make_confirmation_upsert_query(
                            item_hash=item_hash,
                            tx_hash=confirmation.hash,
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
                return MessageProcessingStatus.FAILED_PERMANENTLY, []

            except (ContentCurrentlyUnavailable, Exception) as e:
                if not isinstance(e, ContentCurrentlyUnavailable):
                    LOGGER.exception("Can't get content of object %r" % item_hash)
                async with self.session_factory() as session:
                    await self._mark_message_for_retry(
                        session=session,
                        pending_message=pending_message,
                        retrying=retrying,
                    )
                    await session.commit()
                return MessageProcessingStatus.RETRYING_LATER, []

            validated_message = MessageDb.from_pending_message(
                pending_message=pending_message,
                content_dict=content.value,
                content_size=len(content.raw_value),
            )

            async with self.session_factory() as session:
                try:
                    handling_result, ops = await self.process_message_content(
                        session=session, message=validated_message
                    )
                except UnknownHashError:
                    LOGGER.warning(
                        f"Invalid IPFS hash for message {item_hash}, won't retry."
                    )
                    return MessageProcessingStatus.FAILED_PERMANENTLY, []
                except Exception:
                    LOGGER.exception("Error using the message type handler")
                    handling_result, ops = MessageProcessingStatus.RETRYING_LATER, []

                if handling_result == MessageProcessingStatus.RETRYING_LATER:
                    LOGGER.debug("Message type handler has failed, retrying later.")
                    await self._mark_message_for_retry(
                        session=session,
                        pending_message=pending_message,
                        retrying=retrying,
                    )
                    return MessageProcessingStatus.RETRYING_LATER, ops
                await session.commit()

            if handling_result == MessageProcessingStatus.FAILED_PERMANENTLY:
                LOGGER.warning(
                    "Message type handler has failed permanently for "
                    "%r, won't retry." % item_hash
                )
                return MessageProcessingStatus.FAILED_PERMANENTLY, ops

            if not await check_sender_authorization(
                message=validated_message, content=validated_message.parsed_content
            ):
                LOGGER.warning("Invalid sender for %s" % item_hash)
                return MessageProcessingStatus.MESSAGE_HANDLED, []

            if seen_ids is not None and chain_tx:
                if ids_key in seen_ids.keys():
                    if chain_tx.height > seen_ids[ids_key]:
                        return MessageProcessingStatus.MESSAGE_HANDLED, []
                    else:
                        seen_ids[ids_key] = chain_tx.height
                else:
                    seen_ids[ids_key] = chain_tx.height

            LOGGER.debug("New message to store for %s." % item_hash)

            updates = [
                DbBulkOperation(
                    model=MessageDb,
                    operation=make_message_upsert_query(validated_message),
                )
            ]
            for confirmation in confirmations:
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

            return MessageProcessingStatus.MESSAGE_HANDLED, bulk_ops

        return MessageProcessingStatus.MESSAGE_HANDLED, []

    async def fetch_and_process_one_message_db(self, pending_message: PendingMessageDb):
        # Fetch
        status, ops = await self.incoming(pending_message=pending_message)

        async with self.session_factory() as session:
            async with session.begin():
                for op in ops:
                    await session.execute(op.operation)

            await session.commit()

            # Process
            content_handler = self.get_content_handler(pending_message.message_type)
            # TODO: avoid reloading from the DB, if possible? The idea of having two
            #       loops might make this impossible to avoid.
            message = await get_message_by_item_hash(
                session=session, item_hash=pending_message.item_hash
            )
            await content_handler.process(messages=[message])

    # TODO: refactor this function to take a PendingMessageDb directly
    async def process_one_message(
        self,
        message: BasePendingMessage,
        chain_tx: Optional[ChainTxDb] = None,
    ):
        """
        Helper function to process a message on the spot.
        """

        pending_message = PendingMessageDb.from_obj(message)
        pending_message.tx = chain_tx
        await self.fetch_and_process_one_message_db(pending_message)
