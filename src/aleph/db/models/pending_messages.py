import datetime as dt
from typing import Optional, Any, Dict, Mapping

from aleph_message.models import Chain, MessageType, ItemType
from sqlalchemy import (
    Boolean,
    BigInteger,
    Column,
    TIMESTAMP,
    String,
    Integer,
    ForeignKey,
    Index,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from sqlalchemy_utils.types.choice import ChoiceType

from aleph.schemas.pending_messages import BasePendingMessage
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.channel import Channel
from .base import Base
from .chains import ChainTxDb


class PendingMessageDb(Base):
    """
    A message to be processed by the CCN.
    """

    __tablename__ = "pending_messages"

    id: int = Column(BigInteger, primary_key=True)
    item_hash: str = Column(String, nullable=False)
    type: MessageType = Column(ChoiceType(MessageType), nullable=False)
    chain: Chain = Column(ChoiceType(Chain), nullable=False)
    sender: str = Column(String, nullable=False)
    signature: str = Column(String, nullable=False)
    item_type: ItemType = Column(ChoiceType(ItemType), nullable=False)
    item_content = Column(String, nullable=True)
    content: Optional[Dict[str, Any]] = Column(JSONB, nullable=True)
    time: dt.datetime = Column(TIMESTAMP(timezone=True), nullable=False)
    channel: Optional[Channel] = Column(String, nullable=True)

    reception_time: dt.datetime = Column(TIMESTAMP(timezone=True), nullable=False)
    check_message: bool = Column(Boolean, nullable=False)
    retries: int = Column(Integer, nullable=False)
    tx_hash: Optional[str] = Column(ForeignKey("chain_txs.hash"), nullable=True)
    fetched: bool = Column(Boolean, nullable=False)

    tx: Optional[ChainTxDb] = relationship("ChainTxDb")

    @classmethod
    def from_obj(
        cls,
        obj: BasePendingMessage,
        reception_time: dt.datetime,
        tx_hash: Optional[str] = None,
        check_message: bool = True,
        fetched: bool = False,
    ) -> "PendingMessageDb":
        return cls(
            item_hash=obj.item_hash,
            type=obj.type,
            chain=obj.chain,
            sender=obj.sender,
            signature=obj.signature,
            item_type=obj.item_type,
            item_content=obj.item_content,
            time=timestamp_to_datetime(obj.time),
            channel=Channel(obj.channel) if obj.channel is not None else None,
            check_message=check_message,
            retries=0,
            tx_hash=tx_hash,
            reception_time=reception_time,
            fetched=fetched,
        )

    @classmethod
    def from_message_dict(
        cls,
        message_dict: Mapping[str, Any],
        reception_time: dt.datetime,
        fetched: bool,
        tx_hash: Optional[str] = None,
        check_message: bool = True,
    ) -> "PendingMessageDb":
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
            time=timestamp_to_datetime(message_dict["time"]),
            channel=message_dict.get("channel"),
            check_message=check_message,
            fetched=fetched,
            retries=0,
            tx_hash=tx_hash,
            reception_time=reception_time,
        )


# Used when processing pending messages
Index("ix_retries_time", PendingMessageDb.retries.asc(), PendingMessageDb.time.asc())
