from typing import List, Tuple

from aleph.db.models import MessageDb
from aleph.handlers.content.content_handler import ContentHandler
from aleph.types.db_session import DbSession


class ProgramMessageHandler(ContentHandler):
    async def process(
        self, session: DbSession, messages: List[MessageDb]
    ) -> Tuple[List[MessageDb], List[MessageDb]]:

        for message in messages:
            await self.check_permissions(session=session, message=message)

        return messages, []
