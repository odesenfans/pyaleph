from sqlalchemy.ext.asyncio import AsyncSession
from ..models.file_pins import FilePinDb
from sqlalchemy import exists


async def is_pinned_file(session: AsyncSession, file_hash: str) -> bool:
    exists_stmt = exists(FilePinDb.file_hash).select().where(FilePinDb.file_hash == file_hash)
    result = (await session.execute(exists_stmt)).scalar()
    return result is not None
