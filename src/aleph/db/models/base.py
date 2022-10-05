from typing import Dict, Any, Optional, Set

from sqlalchemy import Table, exists, text
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
    async def count(cls, session: DbSession):
        return (
            session.execute(
                f"SELECT COUNT(*) FROM {cls.__tablename__}"  # type: ignore
            )
        ).scalar_one()

    # TODO: set type of "where" to the SQLA boolean expression class
    @classmethod
    async def exists(cls, session: DbSession, where):

        exists_stmt = exists(text("1")).select().where(where)
        result = (session.execute(exists_stmt)).scalar()
        return result is not None


Base = declarative_base(cls=AugmentedBase)
