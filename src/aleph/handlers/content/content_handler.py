import abc
from typing import Optional

from aleph_message.models import BaseContent

from aleph.db.models import MessageDb


class ContentHandler(abc.ABC):
    @abc.abstractmethod
    async def handle_content(self, message: MessageDb, content: BaseContent) -> Optional[bool]:
        ...
