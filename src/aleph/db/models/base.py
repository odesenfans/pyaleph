from typing import Dict, Any

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import declarative_base


class Base:
    def to_dict(self) -> Dict[str, Any]:
        return {
            column.name: getattr(self, column.name) for column in self.__table__.columns
        }

    @classmethod
    async def count(cls, session: AsyncSession):
        return (
            await session.execute(f"SELECT COUNT(*) FROM {cls.__tablename__}")
        ).scalar_one()


Base = declarative_base(cls=Base)
