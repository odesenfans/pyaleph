from enum import Enum, IntEnum


class MessageStatus(str, Enum):
    PENDING = "pending"
    FETCHED = "fetched"
    PROCESSED = "processed"
    REJECTED = "rejected"
    FORGOTTEN = "forgotten"


class MessageProcessingStatus(IntEnum):
    FAILED_PERMANENTLY = -1
    RETRYING_LATER = 0
    MESSAGE_HANDLED = 1
