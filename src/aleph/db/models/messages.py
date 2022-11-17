import datetime as dt
from typing import Any, Dict, List, Optional, Type, Union

from aleph_message.models import (
    Chain,
    MessageType,
    ItemType,
    BaseContent,
    # AggregateContent,
    ForgetContent,
    PostContent,
    ProgramContent,
    StoreContent,
    AggregateContentKey,
)
from pydantic import Field, Extra, ValidationError
from pydantic.error_wrappers import ErrorWrapper
from sqlalchemy import (
    Column,
    TIMESTAMP,
    String,
    Integer,
    ForeignKey,
    UniqueConstraint,
    ARRAY,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from sqlalchemy_utils.types.choice import ChoiceType

from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.channel import Channel
from aleph.types.message_status import MessageStatus
from .base import Base
from .chains import ChainTxDb
from .pending_messages import PendingMessageDb


# TODO: remove once aleph-message is updated
class AggregateContent(BaseContent):
    """Content of an AGGREGATE message"""

    key: Union[str, AggregateContentKey] = Field(
        description="The aggregate key can be either a string of a dict containing the key in field 'name'"
    )
    content: Dict = Field(description="The content of an aggregate must be a dict")

    class Config:
        extra = Extra.forbid


CONTENT_TYPE_MAP: Dict[MessageType, Type[BaseContent]] = {
    MessageType.aggregate: AggregateContent,
    MessageType.forget: ForgetContent,
    MessageType.post: PostContent,
    MessageType.program: ProgramContent,
    MessageType.store: StoreContent,
}


def validate_message_content(
    message_type: MessageType,
    content_dict: Dict[str, Any],
) -> BaseContent:

    content_type = CONTENT_TYPE_MAP[message_type]
    content = content_type.parse_obj(content_dict)
    # Validate that the content time can be converted to datetime. This will
    # raise a ValueError and be caught
    # TODO: move this validation in aleph-message
    try:
        _ = dt.datetime.fromtimestamp(content_dict["time"])
    except ValueError as e:
        raise ValidationError([ErrorWrapper(e, loc="time")], model=content_type) from e

    return content


class MessageStatusDb(Base):
    __tablename__ = "message_status"

    item_hash: str = Column(String, primary_key=True)
    status: MessageStatus = Column(ChoiceType(MessageStatus), nullable=False)
    reception_time: dt.datetime = Column(TIMESTAMP(timezone=True), nullable=False)


class MessageDb(Base):
    """
    A message that was processed and validated by the CCN.
    """

    __tablename__ = "messages"

    item_hash: str = Column(String, primary_key=True)
    type: MessageType = Column(ChoiceType(MessageType), nullable=False)
    chain: Chain = Column(ChoiceType(Chain), nullable=False)
    sender: str = Column(String, nullable=False, index=True)
    signature: str = Column(String, nullable=False)
    item_type: ItemType = Column(ChoiceType(ItemType), nullable=False)
    item_content: Optional[str] = Column(String, nullable=True)
    content: Any = Column(JSONB, nullable=False)
    time: dt.datetime = Column(TIMESTAMP(timezone=True), nullable=False, index=True)
    channel: Optional[Channel] = Column(String, nullable=True, index=True)
    size: int = Column(Integer, nullable=False)

    confirmations: "List[MessageConfirmationDb]" = relationship(
        "MessageConfirmationDb", back_populates="message"
    )

    _parsed_content: Optional[BaseContent] = None

    @property
    def confirmed(self) -> bool:
        return bool(self.confirmations)

    @property
    def parsed_content(self):
        if self._parsed_content is None:
            self._parsed_content = validate_message_content(self.type, self.content)
        return self._parsed_content

    @staticmethod
    def _coerce_content(
        pending_message: PendingMessageDb, content_dict: Dict[str, Any]
    ) -> Dict[str, Any]:
        if content_dict.get("address") is None:
            content_dict["address"] = pending_message.sender
        if content_dict.get("time") is None:
            content_dict["time"] = pending_message.time.timestamp()
        return content_dict

    @classmethod
    def from_pending_message(
        cls,
        pending_message: PendingMessageDb,
        content_dict: Dict[str, Any],
        content_size: int,
    ) -> "MessageDb":

        content_dict = cls._coerce_content(pending_message, content_dict)
        parsed_content = validate_message_content(pending_message.type, content_dict)

        message = cls(
            item_hash=pending_message.item_hash,
            type=pending_message.type,
            chain=pending_message.chain,
            sender=pending_message.sender,
            signature=pending_message.signature,
            item_type=pending_message.item_type,
            item_content=pending_message.item_content,
            content=content_dict,
            time=pending_message.time,
            channel=pending_message.channel,
            size=content_size,
        )
        message._parsed_content = parsed_content
        return message

    @classmethod
    def from_message_dict(cls, message_dict: Dict[str, Any]) -> "MessageDb":
        """
        Utility function to translate Aleph message dictionaries, such as those returned by the API,
        in the corresponding DB object.
        """

        item_hash = message_dict["item_hash"]

        return cls(
            item_hash=item_hash,
            type=message_dict["type"],
            chain=Chain(message_dict["chain"]),
            sender=message_dict["sender"],
            signature=message_dict["signature"],
            item_type=ItemType(message_dict.get("item_type", ItemType.inline)),
            item_content=message_dict.get("item_content"),
            content=message_dict["content"],
            time=timestamp_to_datetime(message_dict["time"]),
            channel=message_dict.get("channel"),
            size=message_dict.get("size", 0),
        )


# TODO: move these to their own files?
class ForgottenMessageDb(Base):
    __tablename__ = "forgotten_messages"

    item_hash: str = Column(String, primary_key=True)
    type: MessageType = Column(ChoiceType(MessageType), nullable=False)
    chain: Chain = Column(ChoiceType(Chain), nullable=False)
    sender: str = Column(String, nullable=False, index=True)
    signature: str = Column(String, nullable=False)
    item_type: ItemType = Column(ChoiceType(ItemType), nullable=False)
    time: dt.datetime = Column(TIMESTAMP(timezone=True), nullable=False, index=True)
    channel: Optional[Channel] = Column(String, nullable=True, index=True)
    forgotten_by: List[str] = Column(ARRAY(String), nullable=False)  # type: ignore


class RejectedMessageDb(Base):
    __tablename__ = "rejected_messages"

    item_hash: str = Column(String, primary_key=True)
    message: Dict[str, Any] = Column(JSONB, nullable=False)
    reason: str = Column(String, nullable=False)
    traceback: Optional[str] = Column(String, nullable=True)

class MessageConfirmationDb(Base):
    __tablename__ = "message_confirmations"
    __table_args__ = (UniqueConstraint("item_hash", "tx_hash"),)

    id = Column(Integer, primary_key=True)
    item_hash: str = Column(ForeignKey(MessageDb.item_hash), nullable=False, index=True)
    tx_hash: str = Column(
        ForeignKey("chain_txs.hash", ondelete="CASCADE"), nullable=False
    )

    message: MessageDb = relationship(MessageDb, back_populates="confirmations")
    tx: ChainTxDb = relationship("ChainTxDb")