from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert

from ..models.files import FilePinDb, FileReferenceDb, StoredFileDb
from aleph.types.db_session import DbSession


async def is_pinned_file(session: DbSession, file_hash: str) -> bool:
    return await FilePinDb.exists(
        session=session, where=FilePinDb.file_hash == file_hash
    )


async def upsert_file_pin(session: DbSession, file_hash: str, tx_hash: str):
    upsert_stmt = (
        insert(FilePinDb)
        .values(file_hash=file_hash, tx_hash=tx_hash)
        .on_conflict_do_nothing()
    )
    session.execute(upsert_stmt)


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
        .on_conflict_do_nothing(constraint="files_pkey")
    )
    session.execute(upsert_file_stmt)


async def insert_file_reference(
    session: DbSession, file_hash: str, owner: str, item_hash: str
):
    insert_stmt = insert(FileReferenceDb).values(
        file_hash=file_hash, owner=owner, item_hash=item_hash
    )
    session.execute(insert_stmt)


async def file_reference_exists(session: DbSession, file_hash: str):
    return await FileReferenceDb.exists(
        session=session, where=FileReferenceDb.file_hash == file_hash
    )


async def count_file_references(session: DbSession, file_hash: str):
    select_count_stmt = select(func.count()).select_from(
        select(FileReferenceDb).where(FileReferenceDb.file_hash == file_hash)
    )
    return session.execute(select_count_stmt).scalar_one()


async def delete_file_reference(session: DbSession, item_hash: str):
    delete_stmt = delete(FileReferenceDb).where(FileReferenceDb.item_hash == item_hash)
    session.execute(delete_stmt)


async def delete_file(session: DbSession, file_hash: str):
    delete_stmt = delete(StoredFileDb).where(StoredFileDb.hash == file_hash)
    session.execute(delete_stmt)
