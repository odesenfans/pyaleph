from sqlalchemy.dialects.postgresql import insert

from ..models.files import FilePinDb, StoredFileDb
from ...types.db_session import DbSession


async def is_pinned_file(session: DbSession, file_hash: str) -> bool:
    return await FilePinDb.exists(
        session=session, where=FilePinDb.file_hash == file_hash
    )


def make_upsert_stored_file_query(file: StoredFileDb):
    return (
        insert(StoredFileDb)
        .values(file.to_dict())
        .on_conflict_do_nothing(constraint="files_pkey")
    )


async def upsert_stored_file(session: DbSession, file: StoredFileDb):
    upsert_file_stmt = (
        insert(StoredFileDb)
        .values(file.to_dict(exclude={"id"}))
        .on_conflict_do_nothing(constraint="ix_files_cidv0")
    )
    await session.execute(upsert_file_stmt)
