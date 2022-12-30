import logging
from typing import List, Optional

from aiohttp import web
from aleph_message.models import MessageType, ItemHash, Chain
from pydantic import BaseModel, Field, validator, ValidationError, root_validator

from aleph.db.accessors.messages import get_matching_messages, count_matching_messages
from aleph.types.db_session import DbSessionFactory
from aleph.types.sort_order import SortOrder
from aleph.web.controllers.utils import (
    LIST_FIELD_SEPARATOR,
    Pagination,
    cond_output,
)

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
        ge=10,
        lt=200,
        description="Accepted values for the 'item_hash' field.",
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
        results = await get_matching_messages(
            session, include_confirmations=True, **find_filters
        )
        messages = [message.to_dict(include_confirmations=True) for message in results]

        context = {"messages": messages}

        if pagination_per_page is not None:
            total_msgs = await count_matching_messages(session, **find_filters)

            query_string = request.query_string
            pagination = Pagination(
                pagination_page,
                pagination_per_page,
                total_msgs,
                url_base="/messages/posts/page/",
                query_string=query_string,
            )

            context.update(
                {
                    "pagination": pagination,
                    "pagination_page": pagination_page,
                    "pagination_total": total_msgs,
                    "pagination_per_page": pagination_per_page,
                    "pagination_item": "messages",
                }
            )

    return cond_output(request, context, "TODO.html")


# TODO: reactivate/reimplement messages WS
# async def messages_ws(request: web.Request):
#     ws = web.WebSocketResponse()
#     await ws.prepare(request)
#
#     collection = CappedMessage.collection
#     last_id = None
#
#     query_params = WsMessageQueryParams.parse_obj(request.query)
#     find_filters = query_params.to_mongodb_filters()
#
#     initial_count = query_params.history
#
#     items = [
#         item
#         async for item in collection.find(find_filters)
#         .sort([("$natural", -1)])
#         .limit(initial_count)
#     ]
#     for item in reversed(items):
#         item["_id"] = str(item["_id"])
#
#         last_id = item["_id"]
#         await ws.send_json(item)
#
#     closing = False
#
#     while not closing:
#         try:
#             cursor = collection.find(
#                 {"_id": {"$gt": ObjectId(last_id)}},
#                 cursor_type=CursorType.TAILABLE_AWAIT,
#             )
#             while cursor.alive:
#                 async for item in cursor:
#                     if ws.closed:
#                         closing = True
#                         break
#                     item["_id"] = str(item["_id"])
#
#                     last_id = item["_id"]
#                     await ws.send_json(item)
#
#                 await asyncio.sleep(1)
#
#                 if closing:
#                     break
#
#         except ConnectionResetError:
#             break
#
#         except Exception:
#             if ws.closed:
#                 break
#
#             LOGGER.exception("Error processing")
#             await asyncio.sleep(1)
