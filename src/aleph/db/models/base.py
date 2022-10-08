from typing import Dict, Any, Optional, Set

from sqlalchemy import Table
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import declarative_base


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
    async def count(cls, session: AsyncSession):
        return (
            await session.execute(
                f"SELECT COUNT(*) FROM {cls.__tablename__}"  # type:ignore
            )
        ).scalar_one()


Base = declarative_base(cls=AugmentedBase)
