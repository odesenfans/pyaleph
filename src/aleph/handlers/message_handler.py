import logging
from collections import defaultdict
from typing import Optional, Dict, Iterable, Sequence, Tuple

from aleph_message.models import MessageType
from pydantic import ValidationError

from aleph.chains.chain_service import ChainService
from aleph.db.accessors.messages import (
    make_message_upsert_query,
    make_confirmation_upsert_query,
    get_message_by_item_hash,
)
from aleph.db.models import (
    PendingMessageDb,
    MessageDb,
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
from aleph.schemas.pending_messages import BasePendingMessage
from aleph.storage import StorageService
from aleph.types.db_session import DbSessionFactory, DbSession
from aleph.types.message_status import (
    InvalidMessage,
    InvalidSignature,
    MessageUnavailable,
)

LOGGER = logging.getLogger(__name__)


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

    # TODO typing: make this function generic on message type
    def get_content_handler(
        self, message_type: MessageType
    ) -> Optional[ContentHandler]:
        return self.content_handlers.get(message_type)

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

    async def verify_signature(self, pending_message: PendingMessageDb):
        if pending_message.check_message:
            try:
                # TODO: remove type: ignore by deciding the pending message type
                await self.chain_service.verify_signature(pending_message)  # type: ignore
            except InvalidMessageError:
                raise InvalidSignature(
                    f"Invalid signature for '{pending_message.item_hash}'"
                )

    async def fetch_pending_message(self, pending_message: PendingMessageDb) -> MessageDb:
        item_hash = pending_message.item_hash

        try:
            content = await self.storage_service.get_message_content(pending_message)
        except InvalidContent:
            LOGGER.warning("Can't get content of message %r, won't retry." % item_hash)
            raise InvalidMessage("Can't get content of message %s", item_hash)

        except (ContentCurrentlyUnavailable, Exception) as e:
            if not isinstance(e, ContentCurrentlyUnavailable):
                LOGGER.exception("Can't get content of object %r" % item_hash)
            raise MessageUnavailable(f"Could not fetch content for {item_hash}")

        try:
            validated_message = MessageDb.from_pending_message(
                pending_message=pending_message,
                content_dict=content.value,
                content_size=len(content.raw_value),
            )
        except ValidationError as e:
            raise InvalidMessage(f"Invalid message content: {e}") from e

        return validated_message

    async def fetch_related_content(self, session: DbSession, message: MessageDb):
        content_handler = self.get_content_handler(message.type)
        if content_handler is None:
            return

        try:
            await content_handler.fetch_related_content(
                session=session, message=message
            )
        except UnknownHashError:
            raise InvalidMessage(f"Invalid IPFS hash for message {message.item_hash}")
        except (InvalidMessage, MessageUnavailable):
            raise
        except Exception as e:
            LOGGER.exception("Error using the message type handler")
            raise MessageUnavailable(
                f"Unexpected error while fetching related content of {message.item_hash}"
            ) from e

    @staticmethod
    async def confirm_existing_message(
        session: DbSession,
        existing_message: MessageDb,
        pending_message: PendingMessageDb,
    ):
        if pending_message.signature != existing_message.signature:
            raise InvalidSignature(f"Invalid signature for {pending_message.item_hash}")

        if pending_message.tx_hash:
            confirmation_upsert_query = make_confirmation_upsert_query(
                item_hash=pending_message.item_hash, tx_hash=pending_message.tx_hash
            )
            await session.execute(confirmation_upsert_query)

    async def verify_and_fetch(
        self, session: DbSession, pending_message: PendingMessageDb
    ) -> Optional[MessageDb]:
        item_hash = pending_message.item_hash

        existing_message = await get_message_by_item_hash(
            session=session, item_hash=item_hash
        )

        if existing_message:
            # The message already exists and is validated. The current message is a confirmation.
            await self.confirm_existing_message(
                session=session,
                pending_message=pending_message,
                existing_message=existing_message,
            )
            return None

        await self.verify_signature(pending_message=pending_message)
        validated_message = await self.fetch_pending_message(
            pending_message=pending_message
        )
        await self.fetch_related_content(session=session, message=validated_message)

        # All the content was fetched successfully, we can mark the message as fetched
        message_upsert_query = make_message_upsert_query(message=validated_message)
        await session.execute(message_upsert_query)
        if pending_message.tx_hash:
            confirmation_upsert_query = make_confirmation_upsert_query(
                item_hash=item_hash, tx_hash=pending_message.tx_hash
            )
            await session.execute(confirmation_upsert_query)

        return validated_message

    async def process(
        self, session: DbSession, messages: Iterable[MessageDb]
    ) -> Tuple[Sequence[MessageDb], Sequence[MessageDb]]:

        successes = []
        errors = []
        messages_by_type = defaultdict(list)

        for message in messages:
            # TODO: check permissions and balance
            messages_by_type[message.type].append(message)

        # FORGETs should be last to avoid race conditions. Other message types
        # can be rearranged if deemed necessary, i.e. to treat faster operations at
        # the start.
        for message_type in (
            MessageType.aggregate,
            MessageType.post,
            MessageType.program,
            MessageType.store,
            MessageType.forget,
        ):
            content_handler = self.get_content_handler(message_type)
            if content_handler:
                mtype_successes, mtype_errors = await content_handler.process(
                    session=session, messages=messages_by_type[message_type]
                )
                successes += mtype_successes
                errors += mtype_errors

        return successes, errors

    async def fetch_and_process_one_message_db(self, pending_message: PendingMessageDb):
        # Fetch
        async with self.session_factory() as session:
            validated_message = await self.verify_and_fetch(
                session=session, pending_message=pending_message
            )
            await session.commit()

            if validated_message is None:
                # The pending message was a confirmation, we are done.
                return

            # Process
            await self.process(session=session, messages=[validated_message])
            await session.commit()
