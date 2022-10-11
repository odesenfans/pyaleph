from enum import Enum, IntEnum


class MessageStatus(str, Enum):
    PENDING = "pending"
    FETCHED = "fetched"
    PROCESSED = "processed"
    REJECTED = "rejected"
    FORGOTTEN = "forgotten"


class MessageProcessingStatus(IntEnum):
    NEW_MESSAGE = 1
    NEW_CONFIRMATION = 2
    MESSAGE_ALREADY_PROCESSED = 3


class MessageException(Exception):
    ...


class InvalidMessage(MessageException):
    """
    The message is invalid and should be rejected.
    """

    ...


class InvalidSignature(InvalidMessage):
    """
    The message is invalid, in particular because its signature does not
    match the expected value.
    """

    ...


class PermissionDenied(MessageException):
    """
    The sender does not have the permission to perform the requested operation
    on the specified object.
    """

    ...


class MessageUnavailable(MessageException):
    """
    The message or its related content is not available at the moment.
    """

    ...
