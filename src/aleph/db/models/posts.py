from .base import Base
from sqlalchemy import Column, String, TIMESTAMP, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from typing import Any, Optional
import datetime as dt


class PostDb(Base):
    __tablename__ = "posts"

    item_hash: str = Column(String, primary_key=True)
    owner: str = Column(String, nullable=False, index=True)
    type: Optional[str] = Column(String, nullable=True, index=True)
    ref: Optional[str] = Column(String, nullable=True)
    amends: Optional[str] = Column(
        ForeignKey("posts.item_hash"), nullable=True, index=True
    )
    content: Any = Column(JSONB, nullable=False)
    creation_datetime: dt.datetime = Column(TIMESTAMP(timezone=True), nullable=False)
