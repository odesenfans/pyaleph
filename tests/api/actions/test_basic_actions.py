from typing import Sequence, Optional

import pytest

from aleph.types.actions.action import Action, ActionStatus
from aleph.types.actions.executor import Executor, execute_actions


class TestAction(Action):
    def __init__(self, text: str, dependencies: Optional[Sequence[Action]] = None):
        super().__init__(dependencies=dependencies)
        self.text = text


class TestExecutor(Executor):
    async def execute(self, actions: Sequence[TestAction]):  # type: ignore[override]
        for action in actions:
            print(action.text)


@pytest.mark.asyncio
async def test_action_with_dependencies():
    action_1 = TestAction("Hello")
    action_2 = TestAction("World", dependencies=[action_1])

    await execute_actions(
        actions=[action_1, action_2], executors={TestAction: TestExecutor()}
    )
    assert action_1.status == ActionStatus.DONE
    assert action_2.status == ActionStatus.DONE
