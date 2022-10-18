from typing import Optional, List

from sqlalchemy import BigInteger, Column, String, ForeignKey, TIMESTAMP
from sqlalchemy.orm import relationship
from sqlalchemy_utils import ChoiceType

from aleph.types.file_type import FileType
from .base import Base
import datetime as dt


class StoredFileDb(Base):
    __tablename__ = "files"

    # id: int = Column(BigInteger, primary_key=True)

    hash: str = Column(String, nullable=False, primary_key=True)

    # TODO: compute hash equivalences
    # TODO: unique index for sha256
    # TODO: size constraints for hash fields
    # sha256_hex: Optional[str] = Column(String, nullable=True, index=True)
    # cidv0: str = Column(String, nullable=False, unique=True, index=True)
    # cidv1: str = Column(String, nullable=False, unique=True, index=True)

    # size: int = Column(BigInteger, nullable=False)
    # TODO: compute the size from local storage
    size: Optional[int] = Column(BigInteger, nullable=True)
    type: FileType = Column(ChoiceType(FileType), nullable=False)
    created: dt.datetime = Column(TIMESTAMP(timezone=True), nullable=False)

    references: List["FileReferenceDb"] = relationship(
        "FileReferenceDb", back_populates="file"
    )


class FileReferenceDb(Base):
    __tablename__ = "file_references"

    id: int = Column(BigInteger, primary_key=True)

    # TODO: should point to ID instead
    file_hash: str = Column(ForeignKey(StoredFileDb.hash), nullable=False, index=True)
    owner: str = Column(String, nullable=False)
    item_hash: str = Column(String, nullable=False, unique=True)

    file: StoredFileDb = relationship(StoredFileDb, back_populates="references")


class FilePinDb(Base):
    __tablename__ = "file_pins"

    file_hash = Column(String, primary_key=True)
    tx_hash = Column(String, nullable=False)
