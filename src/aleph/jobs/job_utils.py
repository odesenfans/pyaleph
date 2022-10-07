import asyncio
import logging
from itertools import groupby
from typing import Callable
from typing import Dict
from typing import Iterable, Tuple
from typing import cast

from configmanager import Config
from sqlalchemy.ext.asyncio import AsyncSession

import aleph.config
from aleph.db.bulk_operations import DbBulkOperation
from aleph.model import init_db_globals
from aleph.toolkit.split import split_iterable
from aleph.toolkit.timer import Timer
from aleph.types.db_session import DbSession

LOGGER = logging.getLogger(__name__)


def prepare_loop(config_values: Dict) -> Tuple[asyncio.AbstractEventLoop, Config]:
    """
    Prepares all the global variables (sigh) needed to run an Aleph subprocess.

    :param config_values: Dictionary of config values, as provided by the main process.
    :returns: A preconfigured event loop, and the application config for convenience.
              Use the event loop as event loop of the process as it is used by Motor. Using another
              event loop will cause DB calls to fail.
    """

    loop = asyncio.get_event_loop()

    config = aleph.config.app_config
    config.load_values(config_values)

    init_db_globals(config)
    return loop, config


async def perform_db_operations(
    session: AsyncSession, db_operations: Iterable[DbBulkOperation]
) -> None:
    # Sort the operations by collection name before grouping and executing them.
    sorted_operations = sorted(
        db_operations,
        key=lambda op: op.model.__tablename__,
    )

    # Process updates collection by collection. Note that we use an ugly hack for capped
    # collections where we ignore bulk write errors. These errors are usually caused
    # by the node processing the same message twice in parallel, from different sources.
    # This results in updates to a message with different confirmation fields, and capped
    # collections do not like updates that increase the size of a document.
    # We can safely ignore these errors as long as our only capped collection is
    # CappedMessage. Using unordered bulk writes guarantees that all operations will be
    # attempted, meaning that all the messages that can be inserted will be.
    for model, operations in groupby(sorted_operations, lambda op: op.model):
        with Timer() as timer:
            sql_ops = [op.operation for op in operations]

            for sql_op in sql_ops:
                await session.execute(sql_op)

        LOGGER.warning(
            "Performed %d operations on %s in %.4f seconds",
            len(sql_ops),
            model.__tablename__,
            timer.elapsed(),
        )


async def process_job_results(
    session: DbSession,
    tasks: Iterable[asyncio.Task],  # TODO: switch to a generic type when moving to 3.9+
    on_error: Callable[[BaseException], None],
):
    """
    Processes the result of the pending TX/message tasks.

    Splits successful and failed jobs, handles exceptions and performs
    DB operations.

    :param: session:
    :param tasks: Finished job tasks. Each of these tasks must return a list of
                  DbBulkOperation objects. It is up to the caller to determine
                  when tasks are done, for example by using asyncio.wait.
    :param on_error: Error callback function. This function will be called
                     on each error from one of the tasks.
    """
    successes, errors = split_iterable(tasks, lambda t: t.exception() is None)

    for error in errors:
        # mypy sees Optional[BaseException] otherwise
        exception = cast(BaseException, error.exception())
        on_error(exception)

    db_operations = (op for success in successes for op in success.result())

    await perform_db_operations(session=session, db_operations=db_operations)
