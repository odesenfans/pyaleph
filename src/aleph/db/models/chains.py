import datetime as dt
from typing import Dict, Any

from aleph_message.models import Chain
from sqlalchemy import Column, Integer, String, TIMESTAMP
from sqlalchemy_utils.types.choice import ChoiceType

from aleph.toolkit.timestamp import timestamp_to_datetime
from .base import Base


class ChainSyncStatusDb(Base):
    __tablename__ = "chains_sync_status"

    chain: Chain = Column(ChoiceType(Chain), primary_key=True)
    height: int = Column(Integer, nullable=False)
    last_update: dt.datetime = Column(TIMESTAMP(timezone=True), nullable=False)


class ChainTxDb(Base):
    __tablename__ = "chain_txs"

    hash: str = Column(String, primary_key=True)
    chain: Chain = Column(ChoiceType(Chain), nullable=False)
    height: int = Column(Integer, nullable=False)
    datetime: dt.datetime = Column(TIMESTAMP(timezone=True), nullable=False)
    publisher: str = Column(String, nullable=False)

    @classmethod
    def from_dict(cls, tx_dict: Dict[str, Any]) -> "ChainTxDb":
        return cls(
            hash=tx_dict["hash"],
            chain=Chain(tx_dict["chain"]),
            height=tx_dict["height"],
            datetime=timestamp_to_datetime(tx_dict["time"]),
            publisher=tx_dict["publisher"],
        )
