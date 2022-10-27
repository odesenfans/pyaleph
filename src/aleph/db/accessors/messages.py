import datetime as dt
import traceback
from typing import Optional, Sequence, Union, Iterable

from aleph_message.models import ItemHash, Chain, MessageType
from sqlalchemy import func, select, update, text, delete
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import Insert, Select
from sqlalchemy.sql.elements import literal

from aleph.toolkit.timestamp import coerce_to_datetime
from aleph.types.channel import Channel
from aleph.types.db_session import DbSession
from aleph.types.message_status import MessageStatus, InvalidMessageException
from aleph.types.sort_order import SortOrder
from ..models.chains import ChainTxDb
from ..models.messages import (
    MessageDb,
    MessageConfirmationDb,
    MessageStatusDb,
    ForgottenMessageDb,
    RejectedMessageDb,
)
from ..models.pending_messages import PendingMessageDb


async def get_message_by_item_hash(
    session: DbSession, item_hash: str
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
    return (session.execute(select_stmt)).scalar()


async def message_exists(session: DbSession, item_hash: str) -> bool:
    return await MessageDb.exists(
        session=session,
        where=MessageDb.item_hash == item_hash,
    )


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
        select_stmt = select_stmt.where(MessageDb.type == message_type)
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
        MessageDb.time.desc()
        if sort_order == SortOrder.DESCENDING
        else MessageDb.time.asc()
    )

    select_stmt = select_stmt.order_by(order_by_column).offset((page - 1) * pagination)

    # If pagination == 0, return all matching results
    if pagination:
        select_stmt = select_stmt.limit(pagination)

    return select_stmt


async def count_matching_messages(
    session: DbSession, page: int = 1, pagination: int = 0, **kwargs
) -> int:
    # Note that we deliberately ignore the pagination parameters so that users can pass
    # the same parameters as get_matching_messages and get the total number of messages,
    # not just the number on a page.
    if kwargs:
        select_stmt = make_matching_messages_query(**kwargs, page=1, pagination=0)
        select_count_stmt = select(func.count()).select_from(select_stmt)
        return session.execute(select_count_stmt).scalar_one()

    return await MessageDb.count(session=session)


async def get_matching_messages(
    session: DbSession,
    **kwargs,  # Same as make_matching_messages_query
) -> Iterable[MessageDb]:
    """
    Applies the specified filters on the message table and returns matching entries.
    """
    select_stmt = make_matching_messages_query(**kwargs)
    return (session.execute(select_stmt)).scalars()


async def get_message_stats_by_sender(
    session: DbSession,
    addresses: Optional[Sequence[str]] = None,
):
    select_stmt = select(
        MessageDb.sender, MessageDb.type, func.count().label("nb_messages")
    ).group_by(MessageDb.sender, MessageDb.type)
    if addresses:
        select_stmt = select_stmt.where(MessageDb.sender.in_(addresses))

    return session.execute(select_stmt).all()


# TODO: declare a type that will match the result (something like UnconfirmedMessageDb)
#       and translate the time field to epoch.
async def get_unconfirmed_messages(
    session: DbSession, limit: int = 100, chain: Optional[Chain] = None
) -> Iterable[MessageDb]:

    where_clause = MessageConfirmationDb.item_hash == MessageDb.item_hash
    if chain:
        where_clause = where_clause & (ChainTxDb.chain == chain)

    #         (MessageDb.item_hash,
    #         MessageDb.message_type,
    #         MessageDb.chain,
    #         MessageDb.sender,
    #         MessageDb.signature,
    #         MessageDb.item_type,
    #         MessageDb.item_content,
    #         # TODO: exclude content field
    #         MessageDb.content,
    #         MessageDb.time,
    #         MessageDb.channel,)
    select_stmt = select(MessageDb).where(
        ~select(MessageConfirmationDb.item_hash).where(where_clause).exists()
    )

    return (session.execute(select_stmt.limit(limit))).scalars()


def make_message_upsert_query(message: MessageDb) -> Insert:
    return (
        insert(MessageDb)
        .values(message.to_dict())
        .on_conflict_do_update(
            constraint="messages_pkey",
            set_={"time": func.least(MessageDb.time, message.time)},
        )
    )


def make_confirmation_upsert_query(item_hash: str, tx_hash: str) -> Insert:
    return (
        insert(MessageConfirmationDb)
        .values(item_hash=item_hash, tx_hash=tx_hash)
        .on_conflict_do_nothing()
    )


async def get_message_status(
    session: DbSession, item_hash: str
) -> Optional[MessageStatusDb]:
    return (
        session.execute(
            select(MessageStatusDb).where(MessageStatusDb.item_hash == item_hash)
        )
    ).scalar()


# TODO typing: Find a correct type for `where`
def make_message_status_upsert_query(
    item_hash: str, new_status: MessageStatus, reception_time: dt.datetime, where
):
    return (
        insert(MessageStatusDb)
        .values(item_hash=item_hash, status=new_status, reception_time=reception_time)
        .on_conflict_do_update(
            constraint="message_status_pkey",
            set_={
                "status": new_status,
                "reception_time": func.least(
                    MessageStatusDb.reception_time, reception_time
                ),
            },
            where=where,
        )
    )


async def get_distinct_channels(session: DbSession) -> Iterable[Channel]:
    select_stmt = select(MessageDb.channel).distinct().order_by(MessageDb.channel)
    return (session.execute(select_stmt)).scalars()


async def get_forgotten_message(
    session: DbSession, item_hash: str
) -> Optional[ForgottenMessageDb]:
    return session.execute(
        select(ForgottenMessageDb).where(ForgottenMessageDb.item_hash == item_hash)
    ).scalar()


async def forget_message(session: DbSession, item_hash: str, forget_message_hash: str):
    """
    Marks a processed message as validated.

    Expects the caller to perform checks to determine whether the message is
    in the proper state.

    :param session: DB session.
    :param item_hash: Hash of the message to forget.
    :param forget_message_hash: Hash of the forget message.
    """

    copy_row_stmt = insert(ForgottenMessageDb).from_select(
        [
            "item_hash",
            "type",
            "chain",
            "sender",
            "signature",
            "item_type",
            "time",
            "channel",
            "forgotten_by",
        ],
        select(
            MessageDb.item_hash,
            MessageDb.type,
            MessageDb.chain,
            MessageDb.sender,
            MessageDb.signature,
            MessageDb.item_type,
            MessageDb.time,
            MessageDb.channel,
            literal(f"{{{forget_message_hash}}}"),
        ).where(MessageDb.item_hash == item_hash),
    )
    session.execute(copy_row_stmt)
    session.execute(
        update(MessageStatusDb)
        .values(status=MessageStatus.FORGOTTEN)
        .where(MessageStatusDb.item_hash == item_hash)
    )
    session.execute(delete(MessageDb).where(MessageDb.item_hash == item_hash))


async def append_to_forgotten_by(
    session: DbSession, forgotten_message_hash: str, forget_message_hash: str
):
    update_stmt = (
        update(ForgottenMessageDb)
        .where(ForgottenMessageDb.item_hash == forgotten_message_hash)
        .values(
            forgotten_by=text(
                f"array_append({ForgottenMessageDb.forgotten_by.name}, :forget_hash)"
            )
        )
    )
    session.execute(update_stmt, {"forget_hash": forget_message_hash})


async def reject_message(
    session: DbSession, message: MessageDb, exception: InvalidMessageException
):
    item_hash = message.item_hash

    # TODO: use a Pydantic schema?
    message_dict = message.to_dict()
    message_dict["time"] = message_dict["time"].timestamp()

    session.execute(
        update(MessageStatusDb)
        .values(status=MessageStatus.REJECTED)
        .where(MessageStatusDb.item_hash == item_hash)
    )
    session.execute(
        insert(RejectedMessageDb).values(
            item_hash=item_hash,
            message=message_dict,
            reason=str(exception),
            traceback="\n".join(
                traceback.format_exception(
                    InvalidMessageException, exception, exception.__traceback__
                )
            ),
        )
    )
    session.execute(
        delete(MessageConfirmationDb).where(
            MessageConfirmationDb.item_hash == item_hash
        )
    )
    session.execute(delete(MessageDb).where(MessageDb.item_hash == item_hash))


async def reject_pending_message(
    session: DbSession,
    pending_message: PendingMessageDb,
    reason: str,
    exception: Optional[BaseException] = None,
):
    item_hash = pending_message.item_hash
    message_status = await get_message_status(session=session, item_hash=item_hash)

    delete_pending_message_stmt = delete(PendingMessageDb).where(
        PendingMessageDb.id == pending_message.id
    )

    # The message may already be processed and someone is sending invalid copies.
    # Just drop the pending message.
    if message_status:
        if message_status.status not in (MessageStatus.PENDING, MessageStatus.REJECTED):
            session.execute(delete_pending_message_stmt)
            return

    # TODO: use Pydantic schema
    pending_message_dict = pending_message.to_dict(
        exclude={"id", "check_message", "retries", "tx_hash", "reception_time"}
    )
    pending_message_dict["time"] = pending_message_dict["time"].timestamp()

    session.execute(
        update(MessageStatusDb)
        .values(status=MessageStatus.REJECTED)
        .where(MessageStatusDb.item_hash == item_hash)
    )
    exc_traceback = (
        "\n".join(traceback.format_exception(InvalidMessageException, exception, None))
        if exception
        else None
    )
    insert_rejected_message_stmt = insert(RejectedMessageDb).values(
        item_hash=item_hash,
        message=pending_message_dict,
        reason=reason,
        traceback=exc_traceback,
    )
    upsert_rejected_message_stmt = insert_rejected_message_stmt.on_conflict_do_update(
        constraint="rejected_messages_pkey",
        set_={
            "reason": insert_rejected_message_stmt.excluded.reason,
            "traceback": insert_rejected_message_stmt.excluded.traceback,
        },
    )
    session.execute(upsert_rejected_message_stmt)
    session.execute(delete_pending_message_stmt)


async def get_programs_triggered_by_messages(session: DbSession, sort_order: SortOrder):
    time_column = MessageDb.time
    order_by_column = (
        time_column.desc() if sort_order == SortOrder.DESCENDING else time_column.asc()
    )

    message_selector_expr = MessageDb.content["on", "message"]

    select_stmt = (
        select(
            MessageDb.item_hash,
            message_selector_expr.label("message_subscriptions"),
        )
        .where(
            (MessageDb.type == MessageType.program)
            & (message_selector_expr.is_not(None))
        )
        .order_by(order_by_column)
    )

    return session.execute(select_stmt).all()
