import abc
from typing import List, Tuple

from aleph_message.models import BaseContent

from aleph.db.bulk_operations import DbBulkOperation
from aleph.db.models import MessageDb
from aleph.types.message_status import MessageProcessingStatus


class ContentHandler(abc.ABC):
    @abc.abstractmethod
    async def handle_content(
        self, message: MessageDb, content: BaseContent
    ) -> Tuple[MessageProcessingStatus, List[DbBulkOperation]]:
        ...
