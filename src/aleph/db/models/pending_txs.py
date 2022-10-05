from enum import Enum
from typing import Any

from sqlalchemy import Column, Integer, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from sqlalchemy_utils.types.choice import ChoiceType

from .base import Base
from .chains import ChainTxDb


class ChainSyncProtocol(str, Enum):
    OnChain = "aleph"
    OffChain = "aleph-offchain"


class PendingTxDb(Base):
    __tablename__ = "pending_txs"

    tx_hash: str = Column(ForeignKey("chain_txs.hash"), primary_key=True)
    protocol: ChainSyncProtocol = Column(ChoiceType(ChainSyncProtocol), nullable=False)
    protocol_version = Column(Integer, nullable=False)
    content: Any = Column(JSONB, nullable=False)

    tx: "ChainTxDb" = relationship("ChainTxDb")
