import abc
import itertools
from typing import Sequence, Mapping, Type, Union

from .action import Action, ActionStatus


class Executor(abc.ABC):
    @abc.abstractmethod
    async def execute(self, actions: Sequence[Action]):
        ...


async def execute_actions(
    actions: Sequence[Action], executors: Union[Executor, Mapping[Type[Action], Executor]]
):
    def get_executor(_action_type: Type[Action]) -> Executor:
        if isinstance(executors, Executor):
            return executors
        return executors[_action_type]

    # We want to preserve the initial ordering as much as possible.
    # We use a dictionary because Python does not have ordered sets.
    remaining_actions = dict(zip(actions, [None]*len(actions)))

    while remaining_actions:
        pending_actions = []
        for action in remaining_actions.keys():
            if action.status == ActionStatus.PENDING:
                for dependency in action.dependencies:
                    # Propagate failure
                    if dependency.status == ActionStatus.FAILED:
                        action.status = ActionStatus.FAILED
                        break
                    elif dependency.status == ActionStatus.PENDING:
                        break

                pending_actions.append(action)

        sorted_actions = sorted(pending_actions, key=lambda action: action.__class__.__name__)
        for action_type, actions_by_type in itertools.groupby(
            sorted_actions, key=lambda action: action.__class__
        ):
            executor = get_executor(action_type)
            await executor.execute(list(actions_by_type))

        for action in sorted_actions:
            del remaining_actions[action]
            action.status = ActionStatus.FAILED if action.error else ActionStatus.DONE
