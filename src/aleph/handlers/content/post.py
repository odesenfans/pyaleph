from typing import List, Tuple

from aleph_message.models import PostContent

from aleph.db.models import MessageDb
from aleph.db.models.posts import PostDb
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.db_session import DbSession
from aleph.types.message_status import MessageUnavailable
from .content_handler import ContentHandler


class PostMessageHandler(ContentHandler):
    """
    Handler for POST messages. Posts are simple JSON objects posted by users.
    They can be updated (=amended) by subsequent POSTs using the following rules:

    * the amending post replaces the content of the original entirely
    * the content.type field of the amending post is set to "amend"
    * the content.ref field of the amending post is set to the item hash of
      the original post.

    These rules make POSTs slightly different from AGGREGATEs as the whole content
    is overwritten by amending messages. This handler unpacks the content of each
    POST message and puts it in the `posts` table. Readers are expected to find
    the last version of a post on their own using a DB query. We keep each amend
    in case a user decides to delete a version with a FORGET.
    """

    async def fetch_related_content(
        self, session: DbSession, message: MessageDb
    ) -> None:
        content = message.parsed_content
        assert isinstance(content, PostContent)

        # For amends, ensure that the original message exists
        if content.type == "amend":
            original_post_exists = await PostDb.exists(
                session=session, where=PostDb.item_hash == content.ref
            )
            if not original_post_exists:
                raise MessageUnavailable(
                    f"Post {content.ref} referenced by {message.item_hash} is not yet processed"
                )

    async def process(
        self, session: DbSession, messages: List[MessageDb]
    ) -> Tuple[List[MessageDb], List[MessageDb]]:

        for message in messages:
            await self.check_permissions(session=session, message=message)
            content = message.parsed_content
            assert isinstance(content, PostContent)

            post = PostDb(
                item_hash=message.item_hash,
                owner=content.address,
                type=content.type,
                ref=content.ref,
                amends=content.ref if content.type == "amend" else None,
                content=content.content,
                creation_datetime=timestamp_to_datetime(content.time),
            )
            session.add(post)

        return messages, []
