import abc
from dataclasses import dataclass
from typing import List, Tuple

from aleph.db.bulk_operations import DbBulkOperation
from aleph.db.models import MessageDb
from aleph.types.db_session import DbSession
from aleph.types.message_status import MessageProcessingStatus


# TODO: use this class as returned value for fetch?
@dataclass
class MessageProcessingResult:
    status: MessageProcessingStatus
    ops: List[DbBulkOperation]


class ContentHandler(abc.ABC):
    async def fetch_related_content(
        self, session: DbSession, message: MessageDb
    ) -> Tuple[MessageProcessingStatus, List[DbBulkOperation]]:
        """
        Fetch additional content from the network based on the content of a message.

        The implementation is expected to be stateless in terms of DB operations.
        Other operations like storing a file on disk are allowed.

        Note: this function should only be overridden if the content field of
        a message can contain additional data to fetch. Most message types should
        keep the default implementation.
        """
        return MessageProcessingStatus.MESSAGE_HANDLED, []

    @abc.abstractmethod
    async def process(self, messages: List[MessageDb]) -> MessageProcessingStatus:
        """
        Process several messages of the same type and applies the resulting changes.

        This function is in charge of:
        * checking permissions
        * applying DB updates.
        """
        ...
