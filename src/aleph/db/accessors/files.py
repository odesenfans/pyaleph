from sqlalchemy.ext.asyncio import AsyncSession
from ..models.files import FilePinDb
from sqlalchemy import exists


async def is_pinned_file(session: AsyncSession, file_hash: str) -> bool:
    return await FilePinDb.exists(
        session=session, where=FilePinDb.file_hash == file_hash
    )
