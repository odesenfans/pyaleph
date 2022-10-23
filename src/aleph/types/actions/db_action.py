import abc
from abc import ABC
from typing import Optional, Sequence

from sqlalchemy import delete
from sqlalchemy.dialects.postgresql import insert

from aleph.db.accessors.messages import (
    make_message_upsert_query,
    make_confirmation_upsert_query,
    make_message_status_upsert_query,
)
from aleph.db.models import PendingMessageDb, MessageStatusDb, PendingTxDb, MessageDb
from aleph.types.actions.action import Action
from aleph.types.message_status import MessageStatus


class DbAction(Action, abc.ABC):
    @abc.abstractmethod
    def to_db_statements(self):
        ...


class InsertPendingMessage(DbAction):
    def __init__(
        self,
        pending_message: PendingMessageDb,
        dependencies: Optional[Sequence[Action]] = None,
    ):
        super().__init__(dependencies)
        self.pending_message = pending_message

    def to_db_statements(self) -> Sequence:
        message = self.pending_message
        pending_message_dict = message.to_dict(exclude={"id"})

        return [
            insert(MessageStatusDb)
            .values(item_hash=message.item_hash, status=MessageStatus.PENDING)
            .on_conflict_do_update(
                index_elements=["item_hash"],
                set_={"status": MessageStatus.PENDING},
                where=MessageStatusDb.status == MessageStatus.REJECTED,
            ),
            insert(PendingMessageDb).values(**pending_message_dict),
        ]


class MessageDbAction(DbAction, ABC):
    def __init__(
        self,
        pending_message: PendingMessageDb,
        dependencies: Optional[Sequence[Action]] = None,
    ):
        super().__init__(dependencies)
        self.pending_message = pending_message


class ConfirmMessage(MessageDbAction):
    def __init__(
        self,
        pending_message: PendingMessageDb,
        dependencies: Optional[Sequence[Action]] = None,
    ):
        super().__init__(pending_message=pending_message, dependencies=dependencies)

    def to_db_statements(self):
        statements = [
            delete(PendingMessageDb).where(
                PendingMessageDb.id == self.pending_message.id
            )
        ]

        if tx_hash := self.pending_message.tx_hash:
            statements.append(
                make_confirmation_upsert_query(
                    item_hash=self.pending_message.item_hash, tx_hash=tx_hash
                )
            )

        return statements


class UpsertMessage(MessageDbAction):
    def __init__(
        self,
        message: MessageDb,
        pending_message: PendingMessageDb,
        dependencies: Optional[Sequence[Action]] = None,
    ):
        super().__init__(pending_message=pending_message, dependencies=dependencies)
        self.message = message

    def to_db_statements(self):
        statements = [
            make_message_upsert_query(self.message),
            delete(PendingMessageDb).where(
                PendingMessageDb.id == self.pending_message.id
            ),
            make_message_status_upsert_query(
                item_hash=self.message.item_hash,
                new_status=MessageStatus.FETCHED,
                where=(MessageStatusDb.status == MessageStatus.PENDING),
            ),
        ]

        if tx_hash := self.pending_message.tx_hash:
            statements.append(
                make_confirmation_upsert_query(
                    item_hash=self.message.item_hash, tx_hash=tx_hash
                )
            )

        return statements


class DeletePendingTx(DbAction):
    def __init__(
        self,
        tx_hash: str,
        dependencies: Optional[Sequence[Action]] = None,
    ):
        super().__init__(dependencies=dependencies)
        self.tx_hash = tx_hash

    def to_db_statements(self):
        return [
            delete(PendingTxDb).where(PendingTxDb.tx_hash == self.tx_hash),
        ]
