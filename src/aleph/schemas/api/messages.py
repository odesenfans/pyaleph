import datetime as dt
from typing import Optional, Generic, TypeVar, Literal, List, Any, Union

from aleph_message.models import (
    AggregateContent,
    BaseContent,
    Chain,
    ForgetContent,
    PostContent,
    ProgramContent,
    StoreContent,
)
from aleph_message.models import MessageType, ItemType
from pydantic import BaseModel
from pydantic.generics import GenericModel

MType = TypeVar("MType", bound=MessageType)
ContentType = TypeVar("ContentType", bound=BaseContent)


class MessageConfirmation(BaseModel):
    """Format of the result when a message has been confirmed on a blockchain"""

    class Config:
        orm_mode = True
        json_encoders = {dt.datetime: lambda d: d.timestamp()}

    chain: Chain
    height: int
    hash: str


class BaseMessage(GenericModel, Generic[MType, ContentType]):
    class Config:
        orm_mode = True


    sender: str
    chain: Chain
    signature: str
    type: MType
    item_content: Optional[str]
    item_type: ItemType
    item_hash: str
    time: dt.datetime
    channel: Optional[str] = None
    content: ContentType
    confirmed: bool
    confirmations: List[MessageConfirmation]


class AggregateMessage(
    BaseMessage[Literal[MessageType.aggregate], AggregateContent]  # type: ignore
):
    class Config:
        orm_mode = True
        json_encoders = {dt.datetime: lambda d: d.timestamp()}


class ForgetMessage(
    BaseMessage[Literal[MessageType.forget], ForgetContent]  # type: ignore
):
    ...


class PostMessage(BaseMessage[Literal[MessageType.post], PostContent]):  # type: ignore
    ...


class ProgramMessage(
    BaseMessage[Literal[MessageType.program], ProgramContent]  # type: ignore
):
    ...


class StoreMessage(
    BaseMessage[Literal[MessageType.store], StoreContent]  # type: ignore
):
    ...


MESSAGE_CLS_DICT = {
    MessageType.aggregate: AggregateMessage,
    MessageType.forget: ForgetMessage,
    MessageType.post: PostMessage,
    MessageType.program: ProgramMessage,
    MessageType.store: StoreMessage,
}


def format_message(message: Any) -> BaseMessage:
    message_cls = MESSAGE_CLS_DICT[message.type]
    return message_cls.from_orm(message)


AlephMessage = Union[
    AggregateMessage, ForgetMessage, PostMessage, ProgramMessage, StoreMessage
]


class MessageListResponse(BaseModel):
    class Config:
        json_encoders = {dt.datetime: lambda d: d.timestamp()}

    messages: List[AlephMessage]
    pagination_page: int
    pagination_total: int
    pagination_per_page: int
    pagination_item: Literal["messages"] = "messages"
