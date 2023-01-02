import logging
from typing import List, Optional

import aio_pika.abc
from aiohttp import web
from aleph_message.models import MessageType, ItemHash, Chain
from configmanager import Config
from pydantic import BaseModel, Field, validator, ValidationError, root_validator

import aleph.toolkit.json as aleph_json
from aleph.db.accessors.messages import get_matching_messages, count_matching_messages
from aleph.schemas.api.messages import format_message, MessageListResponse
from aleph.types.db_session import DbSessionFactory
from aleph.types.sort_order import SortOrder
from aleph.web.controllers.utils import LIST_FIELD_SEPARATOR

LOGGER = logging.getLogger(__name__)


DEFAULT_MESSAGES_PER_PAGE = 20
DEFAULT_PAGE = 1
DEFAULT_WS_HISTORY = 10


class BaseMessageQueryParams(BaseModel):
    sort_order: SortOrder = Field(
        default=SortOrder.DESCENDING,
        description="Order in which messages should be listed: "
        "-1 means most recent messages first, 1 means older messages first.",
    )
    message_type: Optional[MessageType] = Field(
        default=None, alias="msgType", description="Message type."
    )
    addresses: Optional[List[str]] = Field(
        default=None, description="Accepted values for the 'sender' field."
    )
    refs: Optional[List[str]] = Field(
        default=None, description="Accepted values for the 'content.ref' field."
    )
    content_hashes: Optional[List[ItemHash]] = Field(
        default=None,
        alias="contentHashes",
        description="Accepted values for the 'content.item_hash' field.",
    )
    content_keys: Optional[List[ItemHash]] = Field(
        default=None,
        alias="contentKeys",
        description="Accepted values for the 'content.keys' field.",
    )
    content_types: Optional[List[ItemHash]] = Field(
        default=None,
        alias="contentTypes",
        description="Accepted values for the 'content.type' field.",
    )
    chains: Optional[List[Chain]] = Field(
        default=None, description="Accepted values for the 'chain' field."
    )
    channels: Optional[List[str]] = Field(
        default=None, description="Accepted values for the 'channel' field."
    )
    tags: Optional[List[str]] = Field(
        default=None, description="Accepted values for the 'content.content.tag' field."
    )
    hashes: Optional[List[ItemHash]] = Field(
        default=None, description="Accepted values for the 'item_hash' field."
    )

    @root_validator
    def validate_field_dependencies(cls, values):
        start_date = values.get("start_date")
        end_date = values.get("end_date")
        if start_date and end_date and (end_date < start_date):
            raise ValueError("end date cannot be lower than start date.")
        return values

    @validator(
        "hashes",
        "addresses",
        "content_hashes",
        "content_keys",
        "content_types",
        "chains",
        "channels",
        "tags",
        pre=True,
    )
    def split_str(cls, v):
        if isinstance(v, str):
            return v.split(LIST_FIELD_SEPARATOR)
        return v


class MessageQueryParams(BaseMessageQueryParams):
    pagination: int = Field(
        default=DEFAULT_MESSAGES_PER_PAGE,
        ge=0,
        description="Maximum number of messages to return. Specifying 0 removes this limit.",
    )
    page: int = Field(
        default=DEFAULT_PAGE, ge=1, description="Offset in pages. Starts at 1."
    )

    start_date: float = Field(
        default=0,
        ge=0,
        alias="startDate",
        description="Start date timestamp. If specified, only messages with "
        "a time field greater or equal to this value will be returned.",
    )
    end_date: float = Field(
        default=0,
        ge=0,
        alias="endDate",
        description="End date timestamp. If specified, only messages with "
        "a time field lower than this value will be returned.",
    )


class WsMessageQueryParams(BaseMessageQueryParams):
    history: Optional[int] = Field(
        DEFAULT_WS_HISTORY,
        ge=0,
        lt=200,
        description="Historical elements to send through the websocket.",
    )


async def view_messages_list(request):
    """Messages list view with filters"""

    try:
        query_params = MessageQueryParams.parse_obj(request.query)
    except ValidationError as e:
        raise web.HTTPUnprocessableEntity(body=e.json(indent=4))

    # If called from the messages/page/{page}.json endpoint, override the page
    # parameters with the URL one
    if url_page_param := request.match_info.get("page"):
        query_params.page = int(url_page_param)

    find_filters = query_params.dict(exclude_none=True)

    pagination_page = query_params.page
    pagination_per_page = query_params.pagination

    session_factory: DbSessionFactory = request.app["session_factory"]
    with session_factory() as session:
        messages = get_matching_messages(
            session, include_confirmations=True, **find_filters
        )

        formatted_messages = [
            format_message(message).dict(exclude_defaults=True) for message in messages
        ]

        total_msgs = count_matching_messages(session, **find_filters)

        response = MessageListResponse.construct(
            messages=formatted_messages,
            pagination_page=pagination_page,
            pagination_total=total_msgs,
            pagination_per_page=pagination_per_page,
        )

    return web.json_response(text=response.json())


async def declare_mq_queue(
    mq_conn: aio_pika.abc.AbstractConnection, config: Config
) -> aio_pika.abc.AbstractQueue:
    channel = await mq_conn.channel()
    mq_message_exchange = await channel.declare_exchange(
        name=config.rabbitmq.message_exchange.value,
        type=aio_pika.ExchangeType.FANOUT,
        auto_delete=False,
    )
    mq_queue = await channel.declare_queue(auto_delete=True)
    await mq_queue.bind(mq_message_exchange)
    return mq_queue


async def messages_ws(request: web.Request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    mq_conn: aio_pika.abc.AbstractConnection = request.app["mq_conn"]
    session_factory: DbSessionFactory = request.app["session_factory"]
    config = request.app["config"]
    mq_queue = await declare_mq_queue(mq_conn, config)

    query_params = WsMessageQueryParams.parse_obj(request.query)
    find_filters = query_params.dict(exclude_none=True)

    history = query_params.history

    if history:
        with session_factory() as session:
            messages = get_matching_messages(
                session=session,
                pagination=history,
                include_confirmations=True,
                **find_filters,
            )
            for message in messages:
                await ws.send_str(format_message(message).json())

    try:
        async with mq_queue.iterator() as queue_iter:
            async for mq_message in queue_iter:
                if ws.closed:
                    break

                await mq_message.ack()
                item_hash = aleph_json.loads(mq_message.body)["item_hash"]
                # A bastardized way to apply the filters on the message as well.
                # TODO: put the filter key/values in the RabbitMQ message?
                with session_factory() as session:
                    matching_messages = get_matching_messages(
                        session=session,
                        hashes=[item_hash],
                        include_confirmations=True,
                        **find_filters,
                    )
                    for message in matching_messages:
                        await ws.send_str(format_message(message).json())

    except ConnectionResetError:
        pass
