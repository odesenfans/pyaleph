from typing import Optional, Sequence

from aleph.db.models import PendingMessageDb
from .action import Action


class PendingMessageAction(Action):
    def __init__(
        self,
        pending_message: PendingMessageDb,
        dependencies: Optional[Sequence["Action"]] = None,
    ):
        super().__init__(dependencies)
        self.pending_message = pending_message
