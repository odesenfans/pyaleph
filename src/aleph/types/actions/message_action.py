from typing import Optional, Sequence

from aleph_message.models import ItemHash

from aleph.types.actions.action import Action


class MessageAction(Action):
    def __init__(self, message_hash: ItemHash, dependencies: Optional[Sequence[Action]] = None):
        super().__init__(dependencies=dependencies)
        self.message_hash = message_hash
