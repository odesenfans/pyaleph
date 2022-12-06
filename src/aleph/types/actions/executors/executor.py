import abc
import itertools
from typing import Sequence, Mapping, Type, Union, Protocol, TypeVar

from aleph.types.actions.action import Action, ActionStatus
import logging
import psycopg2
import sqlalchemy.exc

LOGGER = logging.getLogger(__name__)


class Executor(abc.ABC):
    @abc.abstractmethod
    async def execute(self, actions: Sequence[Action]):
        ...


A = TypeVar("A", bound=Action)


class ExecutorProtocol(Protocol[A]):
    async def execute(self, actions: Sequence[A]) -> None:
        ...


async def execute_actions(
    actions: Sequence[Action], executors: Union[Executor, Mapping[Type[A], ExecutorProtocol[A]]]
):
    def get_executor(_action_type: Type[Action]) -> ExecutorProtocol:
        if isinstance(executors, Executor):
            return executors
        return executors[_action_type]

    # We want to preserve the initial ordering as much as possible.
    # We use a dictionary because Python does not have ordered sets.
    remaining_actions = dict(zip(actions, [None]*len(actions)))

    while remaining_actions:
        pending_actions = []
        for action in remaining_actions.keys():
            # TODO: optimize this by maintaining an action->dependents dict
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
            actions_by_type_list = list(actions_by_type)
            executor = get_executor(action_type)
            try:
                await executor.execute(actions_by_type_list)
            except (psycopg2.Error, sqlalchemy.exc.SQLAlchemyError):
                LOGGER.warning(
                    "One or more actions failed in batch mode, "
                    "executing %d actions one by one...",
                    len(actions),
                )
            except Exception as e:
                LOGGER.exception("")

            for action in actions_by_type_list:
                try:
                    await executor.execute([action])
                except (psycopg2.Error, sqlalchemy.exc.SQLAlchemyError) as e:
                    LOGGER.exception("Action %s failed", str(action))
                    action.error = e

        for action in sorted_actions:
            del remaining_actions[action]
            action.status = ActionStatus.FAILED if action.error else ActionStatus.DONE
