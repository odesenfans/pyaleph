import logging
from typing import Sequence

from aleph.types.actions.accept_message_actions import AcceptProgramMessageAction
from aleph.types.actions.executors.accept_message import AcceptMessageExecutor

LOGGER = logging.getLogger(__name__)


class AcceptProgramExecutor(AcceptMessageExecutor):
    async def execute(self, actions: Sequence[AcceptProgramMessageAction]):
        with self.session_factory() as session:
            for action in actions:
                self.accept_message(session=session, action=action)

            session.commit()
