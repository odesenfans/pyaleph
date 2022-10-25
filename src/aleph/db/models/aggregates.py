import datetime as dt
from typing import Any

from sqlalchemy import Boolean, Column, ForeignKey, Index, String, TIMESTAMP
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from .base import Base


class AggregateElementDb(Base):
    """
    The individual message contents that make up an aggregate.

    Aggregates are compacted in the `aggregates` table for usage by the API, this table
    is here only to keep track of the history of an aggregate and to recompute it in case
    messages are received out of order.
    """

    __tablename__ = "aggregate_elements"

    item_hash: str = Column(String, primary_key=True)
    key: str = Column(String, nullable=False)
    owner: str = Column(String, nullable=False)
    content: Any = Column(JSONB, nullable=False)
    creation_datetime: dt.datetime = Column(TIMESTAMP(timezone=True), nullable=False)

    __table_args__ = (
        Index("ix_time_desc", creation_datetime.desc()),
        Index("ix_key_owner", key, owner),
    )


class AggregateDb(Base):
    """
    Compacted aggregates, to be served to users.

    Each row of this table contains an aggregate as it stands up to its last revision.
    """

    __tablename__ = "aggregates"

    key: str = Column(String, primary_key=True)
    owner: str = Column(String, primary_key=True)
    content: Any = Column(JSONB, nullable=False)
    creation_datetime: dt.datetime = Column(TIMESTAMP(timezone=True), nullable=False)
    last_revision_hash: str = Column(
        ForeignKey(AggregateElementDb.item_hash), nullable=False
    )
    dirty = Column(Boolean, nullable=False)

    __table_args__ = (Index("ix_aggregates_owner", owner),)

    last_revision: AggregateElementDb = relationship(AggregateElementDb)
