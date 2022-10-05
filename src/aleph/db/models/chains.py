from aleph_message.models import Chain
from sqlalchemy import Column, Integer, TIMESTAMP
from sqlalchemy_utils.types.choice import ChoiceType

from .base import Base


class ChainSyncStatusDb(Base):
    __tablename__ = "chains_sync_status"

    chain = Column(ChoiceType(Chain), primary_key=True)
    height = Column(Integer, nullable=False)
    last_update = Column(TIMESTAMP(timezone=True), nullable=False)
