import datetime as dt
from typing import Optional

from aleph_message.models import Chain, MessageType, ItemType
from sqlalchemy import Boolean, BIGINT, Column, TIMESTAMP, String, Integer, ForeignKey, Index
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

    id: int = Column(BIGINT, primary_key=True)
    item_hash: str = Column(String, nullable=False)
    message_type: MessageType = Column(ChoiceType(MessageType), nullable=False)
    chain: Chain = Column(ChoiceType(Chain), nullable=False)
    sender = Column(String, nullable=False)
    signature = Column(String, nullable=False)
    item_type: ItemType = Column(ChoiceType(ItemType), nullable=False)
    item_content = Column(String, nullable=True)
    time: dt.datetime = Column(TIMESTAMP(timezone=True), nullable=False)
    channel: Optional[Channel] = Column(String, nullable=True)

    check_message = Column(Boolean, nullable=False)
    retries = Column(Integer, nullable=False)
    tx_hash: Optional[str] = Column(ForeignKey("chain_txs.hash"), nullable=True)

    tx: Optional[ChainTxDb] = relationship("ChainTxDb")

    @classmethod
    def from_obj(
        cls,
        obj: BasePendingMessage,
        tx_hash: Optional[str] = None,
        check_message: bool = True,
    ) -> "PendingMessageDb":
        return cls(
            item_hash=obj.item_hash,
            message_type=obj.type,
            chain=obj.chain,
            sender=obj.sender,
            signature=obj.signature,
            item_type=obj.item_type,
            item_content=obj.item_content,
            time=timestamp_to_datetime(obj.time),
            check_message=check_message,
            retries=0,
            tx_hash=tx_hash,
        )

    @property
    def type(self):
        return self.message_type


# Used when processing pending messages
Index("ix_retries_time", PendingMessageDb.retries.asc(), PendingMessageDb.time.asc())
