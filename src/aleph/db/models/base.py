from typing import Dict, Any, Optional, Set, List, Iterable

from sqlalchemy import Table, exists, text, Column, func, select
from sqlalchemy.orm import declarative_base

from aleph.types.db_session import DbSession


class AugmentedBase:
    __tablename__: str
    __table__: Table

    def to_dict(self, exclude: Optional[Set[str]] = None) -> Dict[str, Any]:
        exclude_set = exclude if exclude is not None else set()

        return {
            column.name: getattr(self, column.name)
            for column in self.__table__.columns
            if column.name not in exclude_set
        }

    @classmethod
    def count(cls, session: DbSession) -> int:
        return (
            session.execute(text(f"SELECT COUNT(*) FROM {cls.__tablename__}"))  # type: ignore
        ).scalar_one()

    @classmethod
    def estimated_count(cls, session: DbSession) -> int:
        """
        Returns an approximation of the number of rows in a table.

        SELECT COUNT(*) can be quite slow. There are techniques to retrieve an
        approximation of the number of rows in a table that are much faster.
        Refer to https://wiki.postgresql.org/wiki/Count_estimate for an explanation.

        :param session: DB session object.
        :return: The approximate number of rows in a table.
        """

        estimate = session.execute(
            text(
                f"SELECT reltuples::bigint FROM pg_class WHERE relname = '{cls.__tablename__}'"
            )
        ).scalar_one()
        # The estimate size can be negative
        return max(estimate, 0)

    # TODO: set type of "where" to the SQLA boolean expression class
    @classmethod
    def exists(cls, session: DbSession, where) -> bool:

        exists_stmt = exists(text("1")).select().where(where)
        result = (session.execute(exists_stmt)).scalar()
        return result is not None

    @classmethod
    def jsonb_keys(cls, session: DbSession, column: Column, where) -> Iterable[str]:
        select_stmt = select(func.jsonb_object_keys(column)).where(where)
        return session.execute(select_stmt).scalars()


Base = declarative_base(cls=AugmentedBase)
