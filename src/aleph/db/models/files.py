from enum import Enum
from typing import Optional, List

from sqlalchemy import BigInteger, Column, String, ForeignKey, UniqueConstraint
from sqlalchemy.orm import relationship
from sqlalchemy_utils import ChoiceType

from .base import Base


class FileType(str, Enum):
    FILE = "f"
    DIRECTORY = "d"


class StoredFileDb(Base):
    __tablename__ = "files"

    id: int = Column(BigInteger, primary_key=True)

    hash: Optional[str] = Column(String, nullable=True)
    cidv0: str = Column(String, nullable=False)
    cidv1: str = Column(String, nullable=False)

    size: int = Column(BigInteger, nullable=False)
    type = Column(ChoiceType(FileType), nullable=False)

    references: List["FileReferenceDb"] = relationship("FileReferenceDb", back_populates="file")


class FileReferenceDb(Base):
    __tablename__ = "file_references"

    id: int = Column(BigInteger, primary_key=True)

    file_id: int = Column(ForeignKey(StoredFileDb.id), nullable=False, index=True)
    owner: str = Column(String, nullable=False)
    item_hash: str = Column(String, nullable=False, unique=True)

    file = relationship(StoredFileDb, back_populates="references")


class FilePinDb(Base):
    __tablename__ = "file_pins"

    file_hash = Column(String, primary_key=True)
    tx_hash = Column(String, nullable=False)
