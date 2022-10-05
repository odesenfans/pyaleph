from typing import Optional, Any, Dict

from aleph_message.models import Chain, MessageType, ItemType
from sqlalchemy import Boolean, Column, TIMESTAMP, String, func, Integer, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy_utils.types.choice import ChoiceType

from .base import Base
from aleph.schemas.pending_messages import BasePendingMessage


class PendingMessageDb(Base):
    """
    A message to be processed by the CCN.
    """

    __tablename__ = "pending_messages"

    item_hash = Column(String, primary_key=True)
    message_type = Column(ChoiceType(MessageType), nullable=False)
    chain = Column(ChoiceType(Chain), nullable=False)
    sender = Column(String, nullable=False)
    signature = Column(String, nullable=False)
    item_type = Column(ChoiceType(ItemType), nullable=False)
    item_content = Column(String, nullable=True)
    time = Column(TIMESTAMP(timezone=True), nullable=False)
    # reception_time = Column(
    #     TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    # )
    channel = Column(String, nullable=True)

    check_message = Column(Boolean, nullable=False)
    retries = Column(Integer, nullable=False)
    tx_hash = Column(ForeignKey("chain_txs.hash"), nullable=True)

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
            time=obj.time,
            check_message=check_message,
            retries=0,
            tx_hash=tx_hash,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            column.name: getattr(self, column.name) for column in self.__table__.columns
        }
