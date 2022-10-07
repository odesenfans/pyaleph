from typing import AsyncContextManager, Callable

from sqlalchemy.ext.asyncio import AsyncSession

DbSession = AsyncSession
DbSessionFactory = Callable[[], AsyncContextManager[DbSession]]
