from typing import AsyncContextManager, Callable

from sqlalchemy.ext.asyncio import AsyncSession
from typing_extensions import TypeAlias

DbSession: TypeAlias = AsyncSession
DbSessionFactory = Callable[[], AsyncContextManager[DbSession]]
