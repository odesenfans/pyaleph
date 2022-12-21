from typing import Optional

from sqlalchemy import delete, func, select, update
from sqlalchemy.dialects.postgresql import insert
import datetime as dt

from ..models.files import (
    FilePinDb,
    FileTagDb,
    StoredFileDb,
    TxFilePinDb,
    MessageFilePinDb,
    FilePinType,
)
from aleph.types.db_session import DbSession
from ...types.files import FileTag


async def is_pinned_file(session: DbSession, file_hash: str) -> bool:
    return await FilePinDb.exists(
        session=session, where=FilePinDb.file_hash == file_hash
    )


async def upsert_tx_file_pin(
    session: DbSession, file_hash: str, tx_hash: str, created: dt.datetime
):
    upsert_stmt = (
        insert(TxFilePinDb)
        .values(
            file_hash=file_hash, tx_hash=tx_hash, type=FilePinType.TX, created=created
        )
        .on_conflict_do_nothing()
    )
    session.execute(upsert_stmt)


async def insert_message_file_pin(
    session: DbSession,
    file_hash: str,
    owner: str,
    item_hash: str,
    ref: Optional[str],
    created: dt.datetime,
):
    insert_stmt = insert(MessageFilePinDb).values(
        file_hash=file_hash,
        owner=owner,
        item_hash=item_hash,
        type=FilePinType.MESSAGE,
        ref=ref,
        created=created,
    )
    session.execute(insert_stmt)


async def count_file_pins(session: DbSession, file_hash: str) -> int:
    select_count_stmt = select(func.count()).select_from(
        select(FilePinDb).where(FilePinDb.file_hash == file_hash)
    )
    return session.execute(select_count_stmt).scalar_one()


async def delete_file_pin(session: DbSession, item_hash: str):
    delete_stmt = delete(MessageFilePinDb).where(
        MessageFilePinDb.item_hash == item_hash
    )
    session.execute(delete_stmt)


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


async def delete_file(session: DbSession, file_hash: str):
    delete_stmt = delete(StoredFileDb).where(StoredFileDb.hash == file_hash)
    session.execute(delete_stmt)


async def get_file_tag(session: DbSession, tag: FileTag) -> Optional[FileTagDb]:
    select_stmt = select(FileTagDb).where(FileTagDb.tag == tag)
    return session.execute(select_stmt).scalar()


async def upsert_file_tag(
    session: DbSession,
    tag: FileTag,
    owner: str,
    file_hash: str,
    last_updated: dt.datetime,
):
    insert_stmt = insert(FileTagDb).values(
        tag=tag, owner=owner, file_hash=file_hash, last_updated=last_updated
    )
    upsert_stmt = insert_stmt.on_conflict_do_update(
        constraint="file_tags_pkey",
        set_={"file_hash": file_hash, "last_updated": last_updated},
        where=FileTagDb.last_updated < last_updated,
    )
    session.execute(upsert_stmt)


async def refresh_file_tag(session: DbSession, tag: FileTag):
    coalesced_ref = func.coalesce(MessageFilePinDb.ref, MessageFilePinDb.item_hash)
    select_latest_file_pin_stmt = (
        select(
            coalesced_ref.label("computed_ref"),
            func.max(MessageFilePinDb.created).label("created"),
        )
        .group_by(coalesced_ref)
        .where(coalesced_ref == tag)
    ).subquery()
    select_file_tag_stmt = select(
        coalesced_ref.label("computed_ref"),
        MessageFilePinDb.owner,
        MessageFilePinDb.file_hash,
        MessageFilePinDb.created,
    ).join(
        select_latest_file_pin_stmt,
        (coalesced_ref == select_latest_file_pin_stmt.c.computed_ref)
        & (MessageFilePinDb.created == select_latest_file_pin_stmt.c.created),
    )

    file_tags = session.execute(select_file_tag_stmt).all()

    insert_stmt = insert(FileTagDb).from_select(
        ["tag", "owner", "file_hash", "last_updated"], select_file_tag_stmt
    )
    upsert_stmt = insert_stmt.on_conflict_do_update(
        constraint="file_tags_pkey",
        set_={
            "file_hash": insert_stmt.excluded.file_hash,
            "last_updated": insert_stmt.excluded.last_updated,
        },
    )
    session.execute(delete(FileTagDb).where(FileTagDb.tag == tag))
    session.execute(upsert_stmt)
