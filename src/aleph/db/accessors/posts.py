import datetime as dt
from typing import Optional, Protocol, Dict, Any, Sequence, Union, Iterable, List

from aleph_message.models import ItemHash
from sqlalchemy import func, select, literal_column, TIMESTAMP, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import aliased
from sqlalchemy.sql import Select

from aleph.db.models.posts import PostDb
from aleph.toolkit.timestamp import coerce_to_datetime
from aleph.types.channel import Channel
from aleph.types.db_session import DbSession
from aleph.types.sort_order import SortOrder


class MergedPost(Protocol):
    item_hash: str
    content: Dict[str, Any]
    original_item_hash: str
    original_type: str
    owner: str
    ref: Optional[str]
    channel: Optional[Channel]
    last_updated: dt.datetime
    created: dt.datetime


Amend = aliased(PostDb)
Original = aliased(PostDb)


def make_select_merged_post_stmt() -> Select:
    select_latest_by_amend_stmt = (
        select(
            Amend.amends, func.max(PostDb.creation_datetime).label("latest_amend_time")
        )
        .group_by(Amend.amends)
        .subquery()
    )
    select_latest_amend_stmt = (
        select(
            Amend.item_hash.label("item_hash"),
            Amend.amends.label("amends"),
            Amend.content.label("content"),
            Amend.creation_datetime.label("creation_datetime"),
        )
        .join(
            select_latest_by_amend_stmt,
            (Amend.amends == select_latest_by_amend_stmt.c.amends)
            & (
                Amend.creation_datetime
                == select_latest_by_amend_stmt.c.latest_amend_time
            ),
        )
        .subquery()
    )
    select_merged_post_stmt = (
        select(
            Original.item_hash.label("original_item_hash"),
            func.coalesce(
                select_latest_amend_stmt.c.item_hash, Original.item_hash
            ).label("item_hash"),
            func.coalesce(select_latest_amend_stmt.c.content, Original.content).label(
                "content"
            ),
            Original.owner.label("owner"),
            Original.ref.label("ref"),
            func.coalesce(
                select_latest_amend_stmt.c.creation_datetime, Original.creation_datetime
            ).label("last_updated"),
            Original.channel.label("channel"),
            Original.creation_datetime.label("created"),
            Original.type.label("original_type"),
        ).join(
            select_latest_amend_stmt,
            Original.item_hash == select_latest_amend_stmt.c.amends,
            isouter=True,
        )
    ).where(Original.amends.is_(None))

    return select_merged_post_stmt


async def get_post(session: DbSession, item_hash: str) -> Optional[MergedPost]:
    select_stmt = make_select_merged_post_stmt()
    select_stmt = select_stmt.where(Original.item_hash == str(item_hash))
    return session.execute(select_stmt).one_or_none()


def make_matching_posts_query(
    hashes: Optional[Sequence[ItemHash]] = None,
    addresses: Optional[Sequence[str]] = None,
    refs: Optional[Sequence[str]] = None,
    post_types: Optional[Sequence[str]] = None,
    tags: Optional[Sequence[str]] = None,
    channels: Optional[Sequence[Channel]] = None,
    start_date: Optional[Union[float, dt.datetime]] = None,
    end_date: Optional[Union[float, dt.datetime]] = None,
    sort_order: Optional[SortOrder] = None,
    page: int = 0,
    pagination: int = 20,
) -> Select:
    select_merged_post_subquery = make_select_merged_post_stmt().subquery()
    select_stmt = select(select_merged_post_subquery)

    start_datetime = coerce_to_datetime(start_date)
    end_datetime = coerce_to_datetime(end_date)

    last_updated_column = literal_column("last_updated", TIMESTAMP(timezone=True))

    if hashes:
        select_stmt = select_stmt.where(
            literal_column("original_item_hash", type_=String).in_(hashes)
        )
    if addresses:
        select_stmt = select_stmt.where(literal_column("owner").in_(addresses))
    if refs:
        select_stmt = select_stmt.where(literal_column("ref").in_(refs))
    if post_types:
        select_stmt = select_stmt.where(literal_column("original_type").in_(post_types))
    if tags:
        select_stmt = select_stmt.where(
            literal_column("content", type_=JSONB)["tags"].astext.in_(tags)
        )
    if channels:
        select_stmt = select_stmt.where(literal_column("channel").in_(channels))
    if start_datetime:
        select_stmt = select_stmt.where(last_updated_column >= start_datetime)
    if end_datetime:
        select_stmt = select_stmt.where(last_updated_column < end_datetime)

    if sort_order:
        order_by_column = (
            last_updated_column.desc()
            if sort_order == SortOrder.DESCENDING
            else last_updated_column.asc()
        )
        select_stmt = select_stmt.order_by(order_by_column)

    # If pagination == 0, return all matching results
    if pagination:
        select_stmt = select_stmt.limit(pagination)
    if page:
        select_stmt = select_stmt.offset((page - 1) * pagination)

    return select_stmt


async def count_matching_posts(
    session: DbSession, page: int = 1, pagination: int = 0, **kwargs
) -> int:
    # Note that we deliberately ignore the pagination parameters so that users can pass
    # the same parameters as get_matching_posts and get the total number of posts,
    # not just the number on a page.
    if kwargs:
        select_stmt = make_matching_posts_query(**kwargs, page=1, pagination=0)
    else:
        select_stmt = make_select_merged_post_stmt()

    select_count_stmt = select(func.count()).select_from(select_stmt)
    return session.execute(select_count_stmt).scalar_one()


async def get_matching_posts(
    session: DbSession,
    # Same as make_matching_posts_query
    **kwargs,
) -> List[MergedPost]:
    select_stmt = make_matching_posts_query(**kwargs)
    return session.execute(select_stmt).all()
