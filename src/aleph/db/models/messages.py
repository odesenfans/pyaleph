import datetime as dt
from typing import Any, Dict, List, Optional

from aleph_message.models import Chain, MessageType, ItemType
from sqlalchemy import Column, TIMESTAMP, String, Integer, ForeignKey, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from sqlalchemy_utils.types.choice import ChoiceType

from aleph.toolkit.timestamp import timestamp_to_datetime
from .base import Base
from .chains import ChainTxDb
from aleph.types.channel import Channel


class MessageDb(Base):
    """
    A message that was processed and validated by the CCN.
    """

    __tablename__ = "messages"

    item_hash: str = Column(String, primary_key=True)
    message_type: MessageType = Column(ChoiceType(MessageType), nullable=False)
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

    @property
    def confirmed(self) -> bool:
        return bool(self.confirmations)

    @classmethod
    def from_message_dict(cls, message_dict: Dict[str, Any]) -> "MessageDb":
        """
        Utility function to translate Aleph message dictionaries, such as those returned by the API,
        in the corresponding DB object.
        """

        item_hash = message_dict["item_hash"]

        return cls(
            item_hash=item_hash,
            message_type=message_dict["type"],
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


class MessageConfirmationDb(Base):
    __tablename__ = "message_confirmations"
    __table_args__ = (UniqueConstraint("item_hash", "tx_hash"),)

    id = Column(Integer, primary_key=True)
    item_hash: str = Column(ForeignKey(MessageDb.item_hash), nullable=False, index=True)
    tx_hash: str = Column(ForeignKey("chain_txs.hash"), nullable=False)

    message: MessageDb = relationship(MessageDb, back_populates="confirmations")
    tx: ChainTxDb = relationship("ChainTxDb")
