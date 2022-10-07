import datetime as dt
from typing import Optional, Sequence, Union, Iterable

from aleph_message.models import ItemHash, Chain, MessageType
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import Insert, Select

from aleph.schemas.validated_message import BaseValidatedMessage
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.db_session import DbSession
from aleph.types.sort_order import SortOrder
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


def coerce_to_datetime(
    datetime_or_timestamp: Optional[Union[float, dt.datetime]]
) -> Optional[dt.datetime]:
    # None for datetimes or 0 for timestamps results in returning None
    if datetime_or_timestamp is None or not datetime_or_timestamp:
        return None

    if isinstance(datetime_or_timestamp, dt.datetime):
        return datetime_or_timestamp

    return timestamp_to_datetime(datetime_or_timestamp)


def make_matching_messages_query(
    hashes: Optional[Sequence[ItemHash]] = None,
    addresses: Optional[Sequence[str]] = None,
    refs: Optional[Sequence[str]] = None,
    chains: Optional[Sequence[Chain]] = None,
    message_type: Optional[MessageType] = None,
    start_date: Optional[Union[float, dt.datetime]] = None,
    end_date: Optional[Union[float, dt.datetime]] = None,
    content_hashes: Optional[Sequence[ItemHash]] = None,
    channels: Optional[Sequence[str]] = None,
    sort_order: SortOrder = SortOrder.DESCENDING,
    page: int = 0,
    pagination: int = 20,
    # TODO: remove once all filters are supported
    **kwargs,
) -> Select:
    select_stmt = select(MessageDb)

    start_datetime = coerce_to_datetime(start_date)
    end_datetime = coerce_to_datetime(end_date)

    if hashes:
        select_stmt = select_stmt.where(MessageDb.item_hash.in_(hashes))
    if addresses:
        select_stmt = select_stmt.where(MessageDb.sender.in_(addresses))
    if chains:
        select_stmt = select_stmt.where(MessageDb.chain.in_(chains))
    if message_type:
        select_stmt = select_stmt.where(MessageDb.message_type == message_type)
    if start_datetime:
        select_stmt = select_stmt.where(MessageDb.time >= start_datetime)
    if end_datetime:
        select_stmt = select_stmt.where(MessageDb.time < end_datetime)
    if refs:
        select_stmt = select_stmt.where(MessageDb.content["ref"].astext.in_(refs))
    if content_hashes:
        select_stmt = select_stmt.where(
            MessageDb.content["item_hash"].astext.in_(content_hashes)
        )
    if channels:
        select_stmt = select_stmt.where(MessageDb.channel.in_(channels))

    order_by_column = (
        MessageDb.time.asc()
        if sort_order == SortOrder.DESCENDING
        else MessageDb.time.desc()
    )

    select_stmt = select_stmt.order_by(order_by_column).offset((page - 1) * pagination)

    # If pagination == 0, return all the matching results
    if pagination:
        select_stmt = select_stmt.limit(pagination)

    return select_stmt


async def get_matching_messages(
    session: AsyncSession,
    **kwargs,  # Same as make_matching_messages_query
) -> Iterable[MessageDb]:
    """
    Applies the specified filters on the message table and returns matching entries.
    """
    select_stmt = make_matching_messages_query(**kwargs)
    return (await session.execute(select_stmt)).scalars()


async def count_matching_messages(
    session: DbSession,
    **kwargs,  # Same as make_matching_messages_query
):
    # TODO implement properly
    # count_stmt = make_matching_messages_query(**kwargs).count()
    # return (await session.execute(count_stmt)).scalar()
    return await MessageDb.count(session)


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
