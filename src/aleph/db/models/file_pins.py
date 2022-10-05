from sqlalchemy import Column, String

from .base import Base


class FilePinDb(Base):
    __tablename__ = "file_pins"

    file_hash = Column(String, primary_key=True)
    tx_hash = Column(String, nullable=False)
