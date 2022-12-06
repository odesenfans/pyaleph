import logging
from typing import Sequence

from aleph_message.models import PostContent
from sqlalchemy import update

from aleph.db.models.posts import PostDb
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.actions.accept_message_actions import AcceptForgetMessageAction
from aleph.types.actions.executors.accept_message import AcceptMessageExecutor
from aleph.db.accessors.posts import get_matching_posts
from aleph.types.db_session import DbSession

LOGGER = logging.getLogger(__name__)


class AcceptForgetExecutor(AcceptMessageExecutor):
    async def process_forget(self, session: DbSession, action: AcceptForgetMessageAction):
        # TODO
        ...

    async def execute(self, actions: Sequence[AcceptForgetMessageAction]):
        with self.session_factory() as session:
            for action in actions:
                self.accept_message(session=session, action=action)
                await self.process_forget(session=session, action=action)

            session.commit()
