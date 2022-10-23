from typing import Optional, Sequence
from enum import Enum


class ActionStatus(Enum):
    PENDING = 0
    DONE = 1
    FAILED = 2


class Action:
    def __init__(self, dependencies: Optional[Sequence["Action"]] = None):
        self.status = ActionStatus.PENDING
        self.dependencies = dependencies or []
        self.error = None

    def is_ready(self) -> bool:
        return all(dep.status == ActionStatus.DONE for dep in self.dependencies)
