from dataclasses import dataclass
from typing import Type, Union

from pymongo import DeleteMany, DeleteOne, InsertOne, UpdateMany, UpdateOne
from sqlalchemy.sql import Delete, Insert, Update

from aleph.db.models.base import Base

MongoOperation = Union[DeleteMany, DeleteOne, InsertOne, UpdateMany, UpdateOne]
SqlOperation = Union[Delete, Insert, Update]


@dataclass
class DbBulkOperation:
    model: Type[Base]
    operation: SqlOperation
