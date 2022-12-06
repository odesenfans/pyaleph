import logging
from typing import Sequence

from aleph_message.models import PostContent
from sqlalchemy import update

from aleph.db.models import MessageDb
from aleph.db.models.posts import PostDb
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.actions.accept_message_actions import AcceptPostMessageAction
from aleph.types.actions.executors.accept_message import AcceptMessageExecutor
from aleph.db.accessors.posts import get_matching_posts
from aleph.types.db_session import DbSession

LOGGER = logging.getLogger(__name__)


class AcceptPostExecutor(AcceptMessageExecutor):
    async def insert_post(self, session: DbSession, message: MessageDb):
        content = message.parsed_content
        assert isinstance(content, PostContent)

        creation_datetime = timestamp_to_datetime(content.time)

        post = PostDb(
            item_hash=message.item_hash,
            owner=content.address,
            type=content.type,
            ref=content.ref,
            amends=content.ref if content.type == "amend" else None,
            channel=message.channel,
            content=content.content,
            creation_datetime=creation_datetime,
        )
        session.add(post)

        if content.type == "amend":
            [amended_post] = await get_matching_posts(
                session=session, hashes=[content.ref]
            )
            if amended_post.last_updated < creation_datetime:
                session.execute(
                    update(PostDb)
                        .where(PostDb.item_hash == content.ref)
                        .values(latest_amend=message.item_hash)
                )

    async def execute(self, actions: Sequence[AcceptPostMessageAction]):
        with self.session_factory() as session:
            for action in actions:
                self.accept_message(session=session, action=action)
                await self.insert_post(session=session, message=action.message)

            session.commit()
