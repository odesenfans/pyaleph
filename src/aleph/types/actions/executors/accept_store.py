import logging
from typing import Sequence

from aleph_message.models import StoreContent

from aleph.db.accessors.files import insert_file_reference
from aleph.db.models import MessageDb
from aleph.types.actions.accept_message_actions import AcceptForgetMessageAction
from aleph.types.actions.executors.accept_message import AcceptMessageExecutor
from aleph.types.db_session import DbSession

LOGGER = logging.getLogger(__name__)


async def _create_file_reference(session: DbSession, message: MessageDb):
    content = message.parsed_content
    assert isinstance(content, StoreContent)

    await insert_file_reference(
        session=session,
        file_hash=content.item_hash,
        owner=content.address,
        item_hash=message.item_hash,
    )


class AcceptStoreExecutor(AcceptMessageExecutor):
    async def execute(self, actions: Sequence[AcceptForgetMessageAction]):
        with self.session_factory() as session:
            for action in actions:
                self.accept_message(session=session, action=action)
                await _create_file_reference(session=session, message=action.message)

            session.commit()
