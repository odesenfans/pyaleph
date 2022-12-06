from typing import Optional, Sequence

from aleph.db.models.messages import MessageDb
from aleph.db.models.pending_messages import PendingMessageDb
from aleph.types.actions.action import Action
from aleph.types.actions.pending_message_action import PendingMessageAction


class AcceptMessageAction(PendingMessageAction):
    def __init__(
        self,
        pending_message: PendingMessageDb,
        message: MessageDb,
        dependencies: Optional[Sequence[Action]] = None,
    ):
        super().__init__(pending_message=pending_message, dependencies=dependencies)
        self.message = message


class AcceptAggregateMessageAction(AcceptMessageAction):
    ...


class AcceptForgetMessageAction(AcceptMessageAction):
    ...


class AcceptPostMessageAction(AcceptMessageAction):
    ...


class AcceptProgramMessageAction(AcceptMessageAction):
    ...


class AcceptStoreMessageAction(AcceptMessageAction):
    ...
