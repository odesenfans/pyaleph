import datetime as dt
from typing import Optional, Sequence, List

from aleph_message.models import ItemHash, Chain, MessageType
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import Insert

from aleph.schemas.validated_message import BaseValidatedMessage
from aleph.toolkit.timestamp import timestamp_to_datetime
from ..models.messages import MessageDb, MessageConfirmationDb


async def get_message_by_item_hash(
    session: AsyncSession, item_hash: str
) -> Optional[MessageDb]:
    select_stmt = (
        select(MessageDb)
        .where(MessageDb.item_hash == item_hash)
        .options(
            selectinload(MessageDb.confirmations).options(
                selectinload(MessageConfirmationDb.tx)
            )
        )
    )
    return (await session.execute(select_stmt)).scalar()


async def get_matching_messages(
    session: AsyncSession,
    hashes: Optional[Sequence[ItemHash]] = None,
    addresses: Optional[Sequence[str]] = None,
    refs: Optional[Sequence[str]] = None,
    chains: Optional[Sequence[Chain]] = None,
    message_type: Optional[MessageType] = None,
    start_date: Optional[dt.datetime] = None,
    end_date: Optional[dt.datetime] = None,
    content_hashes: Optional[Sequence[ItemHash]] = None,
    page: int = 0,
    entries_per_page: int = 20,
    # TODO: remove once all filters are supported
    **kwargs,
) -> List[MessageDb]:
    """
    Applies the specified filters on the message table and returns matching entries.
    """
    select_stmt = select(MessageDb)

    if hashes:
        select_stmt = select_stmt.where(MessageDb.item_hash.in_(hashes))
    if addresses:
        select_stmt = select_stmt.where(MessageDb.sender.in_(addresses))
    if chains:
        select_stmt = select_stmt.where(MessageDb.chain.in_(chains))
    if message_type:
        select_stmt = select_stmt.where(MessageDb.message_type == message_type)
    if start_date:
        select_stmt = select_stmt.where(MessageDb.time >= start_date)
    if end_date:
        select_stmt = select_stmt.where(MessageDb.time < end_date)
    if refs:
        select_stmt = select_stmt.where(
            MessageDb.content["ref"].astext.in_(refs)
        )
    if content_hashes:
        select_stmt = select_stmt.where(
            MessageDb.content["item_hash"].astext.in_(content_hashes)
        )

    select_stmt = (
        select_stmt.order_by(MessageDb.time)
        .offset(page * entries_per_page)
        .limit(entries_per_page)
    )

    return (await session.execute(select_stmt)).scalars()


def make_message_upsert_query(message: BaseValidatedMessage) -> Insert:
    return (
        insert(MessageDb)
        .values(
            item_hash=message.item_hash,
            message_type=message.type,
            chain=message.chain,
            sender=message.sender,
            signature=message.signature,
            item_type=message.item_type,
            content=message.content.dict(exclude_none=True),
            time=dt.datetime.utcfromtimestamp(message.time),
            channel=message.channel,
            size=message.size,
        )
        .on_conflict_do_update(
            constraint="messages_pkey",
            set_={
                "time": func.least(MessageDb.time, timestamp_to_datetime(message.time))
            },
        )
    )


def make_confirmation_upsert_query(item_hash: str, tx_hash: str) -> Insert:
    return (
        insert(MessageConfirmationDb)
        .values(item_hash=item_hash, tx_hash=tx_hash)
        .on_conflict_do_nothing()
    )
