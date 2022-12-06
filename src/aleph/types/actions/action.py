from enum import Enum
from typing import Optional, Sequence, Dict, Type


class ActionStatus(Enum):
    PENDING = 0
    DONE = 1
    FAILED = 2


class Action:
    status: ActionStatus
    dependencies: Sequence["Action"]
    error: Optional[Exception]
    error_actions: Dict[Type[Exception], "Action"]
    default_error_action: Optional["Action"]

    def __init__(
        self,
        dependencies: Optional[Sequence["Action"]] = None,
    ):
        self.status = ActionStatus.PENDING
        self.dependencies = dependencies or []
        self.error = None
        self.error_actions = {}
        self.default_error_action = None

    def is_ready(self) -> bool:
        return all(dep.status == ActionStatus.DONE for dep in self.dependencies)

    def set_error_action(self, exception_type: Type[Exception], action: "Action"):
        self.error_actions[exception_type] = action

    def set_default_error_action(self, action: "Action"):
        self.default_error_action = action

    def get_error_action(self, error: Exception) -> Optional["Action"]:
        return self.error_actions.get(type(error), self.default_error_action)
