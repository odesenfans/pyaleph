import itertools
import logging
from typing import Sequence

import psycopg2
import sqlalchemy.exc

from aleph.types.actions.db_action import DbAction
from aleph.types.actions.executors.executor import Executor
from aleph.types.db_session import DbSessionFactory

LOGGER = logging.getLogger(__name__)


class DbExecutor(Executor):
    """
    This executor aims to minimize the amount of transactions required to perform a large
    amount of operations on database objects. It will first try to use a single transaction
    to commit all the submitted DB actions/operations. If this transaction fails, it will
    then try to execute each query in its own transaction to detect the ones that failed,
    if any (all statements may be valid but conflict when executed together, ex: upserts).
    """

    def __init__(self, session_factory: DbSessionFactory):
        self.session_factory = session_factory

    async def execute(self, actions: Sequence[DbAction]):   # type: ignore[override]
        db_statements = [action.to_db_statements() for action in actions]

        try:
            with self.session_factory() as session:
                for statement in itertools.chain.from_iterable(db_statements):
                    session.execute(statement)
                session.commit()
            return

        except (psycopg2.Error, sqlalchemy.exc.SQLAlchemyError):
            LOGGER.warning(
                "One or more actions failed in batch mode, "
                "executing %d actions one by one...",
                len(actions),
            )

        for action, statements in zip(actions, db_statements):
            try:
                with self.session_factory() as session:
                    for statement in statements:
                        session.execute(statement)
                    session.commit()

            except (psycopg2.Error, sqlalchemy.exc.SQLAlchemyError) as e:
                LOGGER.exception("Action %s failed", str(action))
                action.error = e
