from typing import Optional, Sequence, Mapping, Type, TypeVar

from aleph.db.models import PendingMessageDb
from .action import Action
from .executors.executor import ExecutorProtocol


class PendingMessageAction(Action):
    def __init__(
        self,
        pending_message: PendingMessageDb,
        dependencies: Optional[Sequence["Action"]] = None,
    ):
        super().__init__(dependencies)
        self.pending_message = pending_message


A = TypeVar("A", bound=PendingMessageAction)


async def process_pending_message_actions(
    actions: Sequence[PendingMessageAction],
    executors: Mapping[Type[A], ExecutorProtocol[A]],
):


    ...
