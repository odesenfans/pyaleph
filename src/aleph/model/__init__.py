from logging import getLogger

from configmanager import Config

try:
    from pymongo import MongoClient
except ImportError:  # pragma: no cover
    # Backward compatibility with PyMongo 2.2
    from pymongo import Connection as MongoClient

from motor.motor_asyncio import AsyncIOMotorClient

LOGGER = getLogger("model")

db_backend = None

# Mongodb connection and db
connection = None
db = None


def init_db_globals(config: Config):
    global connection, db
    connection = AsyncIOMotorClient(config.mongodb.uri.value, tz_aware=True)
    db = connection[config.mongodb.database.value]


def init_db(config: Config, ensure_indexes: bool = True):
    init_db_globals(config)
    sync_connection = MongoClient(config.mongodb.uri.value, tz_aware=True)
    sync_db = sync_connection[config.mongodb.database.value]

    if ensure_indexes:
        LOGGER.info("Inserting indexes")
        from aleph.model.messages import Message

        Message.ensure_indexes(sync_db)

    from aleph.model.messages import Message
