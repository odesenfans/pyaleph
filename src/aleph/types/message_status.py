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


class InvalidMessageException(MessageException):
    """
    The message is invalid and should be rejected.
    """

    ...


class InvalidSignature(InvalidMessageException):
    """
    The message is invalid, in particular because its signature does not
    match the expected value.
    """

    ...


class PermissionDenied(InvalidMessageException):
    """
    The sender does not have the permission to perform the requested operation
    on the specified object.
    """

    ...


class RetryMessageException(MessageException):
    """
    The message should be retried.
    """
    ...


class MissingDependency(RetryMessageException):
    """
    An object targeted by the message is missing.
    """
    ...


class MessageUnavailable(RetryMessageException):
    """
    The message or its related content is not available at the moment.
    """

    ...
