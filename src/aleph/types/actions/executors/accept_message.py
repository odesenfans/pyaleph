from typing import Sequence

from sqlalchemy import delete

from aleph.db.accessors.messages import (
    make_message_upsert_query,
    make_message_status_upsert_query,
    make_confirmation_upsert_query,
)
from aleph.db.models import PendingMessageDb, MessageStatusDb
from aleph.types.actions.accept_message_actions import AcceptMessageAction
from aleph.types.db_session import DbSessionFactory, DbSession
from aleph.types.message_status import MessageStatus


class AcceptMessageExecutor:
    def __init__(self, session_factory: DbSessionFactory):
        self.session_factory = session_factory

    @staticmethod
    def accept_message(session: DbSession, action: AcceptMessageAction):
        session.execute(make_message_upsert_query(action.message))
        session.execute(
            delete(PendingMessageDb).where(
                PendingMessageDb.id == action.pending_message.id
            )
        )
        session.execute(
            make_message_status_upsert_query(
                item_hash=action.pending_message.item_hash,
                new_status=MessageStatus.FETCHED,
                reception_time=action.pending_message.reception_time,
                where=(MessageStatusDb.status == MessageStatus.PENDING),
            )
        )

        if tx_hash := action.pending_message.tx_hash:
            session.execute(
                make_confirmation_upsert_query(
                    item_hash=action.pending_message.item_hash, tx_hash=tx_hash
                )
            )
