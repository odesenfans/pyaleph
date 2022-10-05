from enum import Enum

from sqlalchemy import Column, Integer, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from sqlalchemy_utils.types.choice import ChoiceType

from .base import Base


class ChainSyncProtocol(str, Enum):
    OnChain = "aleph"
    OffChain = "aleph-offchain"


class PendingTxDb(Base):
    __tablename__ = "pending_txs"

    tx_hash = Column(ForeignKey("chain_txs.hash"), primary_key=True)
    protocol = Column(ChoiceType(ChainSyncProtocol), nullable=False)
    protocol_version = Column(Integer, nullable=False)
    content = Column(JSONB, nullable=False)

    tx = relationship("ChainTxDb")
