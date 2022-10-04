from enum import Enum

from sqlalchemy import Column, String, TIMESTAMP
from sqlalchemy_utils.types.choice import ChoiceType

from .base import Base


class PeerType(str, Enum):
    HTTP = "HTTP"
    IPFS = "IPFS"
    P2P = "P2P"


class PeerDb(Base):
    __tablename__ = "peers"

    peer_id = Column(String, primary_key=True)
    peer_type = Column(ChoiceType(PeerType), primary_key=True)
    address = Column(String, nullable=False)
    source = Column(ChoiceType(PeerType), nullable=False)
    last_seen = Column(TIMESTAMP(timezone=True), nullable=False)
