"""
Schemas for validated messages, as stored in the messages collection.
Validated messages are fully loaded, i.e. their content field is
always present, unlike pending messages.
"""

from typing import List, Literal, Optional, Generic, Dict, Type, Any, Union

from aleph_message.models import (
    MessageConfirmation,
    AggregateContent,
    ForgetContent,
    MessageType,
    PostContent,
    ProgramContent,
    StoreContent,
)
from pydantic import BaseModel, Field

from aleph.schemas.base_messages import AlephBaseMessage, ContentType, MType
from aleph.schemas.pending_messages import (
    BasePendingMessage,
    PendingAggregateMessage,
    PendingForgetMessage,
    PendingPostMessage,
    PendingProgramMessage,
    PendingStoreMessage,
)
from .message_content import MessageContent
from ..db.models import PendingMessageDb


class EngineInfo(BaseModel):
    hash: str = Field(alias="Hash")
    size: int = Field(alias="Size")
    cumulative_size: int = Field(alias="CumulativeSize")
    blocks: int = Field(alias="Blocks")
    type: str = Field(alias="Type")


class StoreContentWithMetadata(StoreContent):
    content_type: Literal["directory", "file"]
    size: int
    engine_info: Optional[EngineInfo] = None

    @classmethod
    def from_content(cls, store_content: StoreContent):
        return cls(
            address=store_content.address,
            time=store_content.time,
            item_type=store_content.item_type,
            item_hash=store_content.item_hash,
            content_type="file",
            size=0,
            engine_info=None,
        )


class BaseValidatedMessage(AlephBaseMessage, Generic[MType, ContentType]):
    confirmed: bool
    size: int
    content: ContentType
    confirmations: List[MessageConfirmation] = Field(default_factory=list)
    forgotten_by: List[str] = Field(default_factory=list)


class ValidatedAggregateMessage(
    BaseValidatedMessage[Literal[MessageType.aggregate], AggregateContent]  # type: ignore
):
    pass


class ValidatedForgetMessage(
    BaseValidatedMessage[Literal[MessageType.forget], ForgetContent]  # type: ignore
):
    pass


class ValidatedPostMessage(
    BaseValidatedMessage[Literal[MessageType.post], PostContent]  # type: ignore
):
    pass


class ValidatedProgramMessage(
    BaseValidatedMessage[Literal[MessageType.program], ProgramContent]  # type: ignore
):
    pass


class ValidatedStoreMessage(
    BaseValidatedMessage[Literal[MessageType.store], StoreContent]  # type: ignore
):
    pass


def validate_pending_message(
    pending_message: Union[BasePendingMessage[MType, ContentType], PendingMessageDb],
    content: MessageContent,
    confirmations: List[MessageConfirmation],
) -> BaseValidatedMessage[MType, ContentType]:

    # TODO: try avoid the union between pydantic and sqla model
    #       and get rid of this patchwork here.
    def pending_message_db_to_dict(_pending_message: PendingMessageDb) -> Dict[str, Any]:
        _m_dict = _pending_message.to_dict()
        del _m_dict["id"]
        del _m_dict["retries"]
        del _m_dict["check_message"]
        del _m_dict["tx_hash"]
        _m_dict["type"] = _m_dict.pop("message_type")
        _m_dict["time"] = _m_dict["time"].timestamp()
        return _m_dict

    type_map: Dict[MessageType, Type[BaseValidatedMessage]] = {
        MessageType.aggregate: ValidatedAggregateMessage,
        MessageType.forget: ValidatedForgetMessage,
        MessageType.post: ValidatedPostMessage,
        MessageType.program: ValidatedProgramMessage,
        MessageType.store: ValidatedStoreMessage,
    }

    # Some values may be missing in the content, adjust them
    json_content = content.value
    if json_content.get("address", None) is None:
        json_content["address"] = pending_message.sender

    if json_content.get("time", None) is None:
        json_content["time"] = pending_message.time

    pending_message_dict = (
        pending_message_db_to_dict(pending_message)
        if isinstance(pending_message, PendingMessageDb)
        else pending_message.dict(exclude={"content"})
    )

    # Note: we could use the construct method of Pydantic to bypass validation
    # and speed up the conversion process. However, this means giving up on validation.
    # At the time of writing, correctness seems more important than performance.
    return type_map[pending_message.type](
        **pending_message_dict,
        content=content.value,
        confirmed=bool(confirmations),
        confirmations=confirmations,
        size=len(content.raw_value),
    )


def make_confirmation_update_query(confirmations: List[MessageConfirmation]) -> Dict:
    """
    Creates a MongoDB update query that confirms an existing message.
    """

    # We use addToSet as multiple confirmations may be treated in //
    if not confirmations:
        return {"$max": {"confirmed": False}}

    return {
        "$max": {"confirmed": True},
        "$addToSet": {
            "confirmations": {
                "$each": [confirmation.dict() for confirmation in confirmations]
            }
        },
    }


def make_message_upsert_query(message: BaseValidatedMessage[Any, Any]) -> Dict:
    """
    Creates a MongoDB upsert query to insert the message in the DB.
    """

    updates = {
        "$set": {
            "content": message.content.dict(exclude_none=True),
            "size": message.size,
            "item_content": message.item_content,
            "item_type": message.item_type.value,
            "channel": message.channel,
            "signature": message.signature,
        },
        "$min": {"time": message.time},
    }

    # Add fields related to confirmations
    updates.update(make_confirmation_update_query(message.confirmations))

    return updates
