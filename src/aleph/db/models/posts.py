from .base import Base
from sqlalchemy import Column, String, TIMESTAMP, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB


class PostDb(Base):
    __tablename__ = "posts"

    item_hash = Column(String, primary_key=True)
    owner = Column(String, nullable=False, index=True)
    type = Column(String, nullable=True, index=True)
    ref = Column(String, nullable=True)
    amends = Column(ForeignKey("posts.item_hash"), nullable=True, index=True)
    content = Column(JSONB, nullable=False)
    creation_datetime = Column(TIMESTAMP(timezone=True), nullable=False)
