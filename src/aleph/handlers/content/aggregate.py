import itertools
import logging
from typing import Tuple, List, cast, Sequence

from aleph_message.models import AggregateContent

from aleph.db.accessors.aggregates import (
    get_aggregate_by_key,
    merge_aggregate_elements,
    insert_aggregate,
    insert_aggregate_element,
    refresh_aggregate,
)
from aleph.db.models import MessageDb, AggregateElementDb
from aleph.handlers.content.content_handler import ContentHandler
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.db_session import DbSessionFactory, DbSession

LOGGER = logging.getLogger(__name__)


class AggregateMessageHandler(ContentHandler):
    def __init__(self, session_factory: DbSessionFactory):
        self.session_factory = session_factory

    async def fetch_related_content(
        self, session: DbSession, message: MessageDb
    ) -> None:
        return

    async def _insert_aggregate_element(self, session: DbSession, message: MessageDb):
        content = cast(AggregateContent, message.parsed_content)
        aggregate_element = AggregateElementDb(
            item_hash=message.item_hash,
            key=content.key,
            owner=content.address,
            content=content.content,
            creation_datetime=timestamp_to_datetime(message.parsed_content.time),
        )

        await insert_aggregate_element(
            session=session,
            item_hash=aggregate_element.item_hash,
            key=aggregate_element.key,
            owner=aggregate_element.owner,
            content=aggregate_element.content,
            creation_datetime=aggregate_element.creation_datetime,
        )

        return aggregate_element

    @staticmethod
    async def _update_aggregate(
        session: DbSession,
        key: str,
        owner: str,
        elements: Sequence[AggregateElementDb],
    ):
        """
        Creates/updates an aggregate with new elements.

        :param session: DB session.
        :param key: Aggregate key.
        :param owner: Aggregate owner.
        :param elements: New elements to insert, ordered by their creation_datetime field.
        :return:
        """
        aggregate = await get_aggregate_by_key(session=session, owner=owner, key=key)

        if not aggregate:
            LOGGER.info("%s/%s does not exist, creating it", key, owner)

            content = merge_aggregate_elements(elements)
            await insert_aggregate(
                session=session,
                key=key,
                owner=owner,
                content=content,
                creation_datetime=elements[0].creation_datetime,
                last_revision_hash=elements[-1].item_hash,
            )
            return

        LOGGER.info("%s/%s already exists, updating it", key, owner)

        # Best case scenario: the elements we are adding are all posterior to the last
        # update, we can just merge the content of aggregate and the new elements.
        if aggregate.last_revision.creation_datetime < elements[0].creation_datetime:

            new_content = merge_aggregate_elements(elements)
            aggregate.content.update(new_content)
            aggregate.last_revision_hash = elements[-1].item_hash

        # Out of order insertions. Here, we need to get all the elements in the database
        # and recompute the aggregate entirely.
        else:
            LOGGER.info("%s/%s: out of order refresh", key, owner)
            # Expect the new elements to already be added to the current session.
            # We flush it to make them accessible from the current transaction.
            session.flush()
            await refresh_aggregate(session=session, owner=owner, key=key)

    async def process(
        self, session: DbSession, messages: List[MessageDb]
    ) -> Tuple[List[MessageDb], List[MessageDb]]:
        sorted_messages = sorted(
            messages,
            key=lambda m: (m.parsed_content.key, m.parsed_content.address, m.time),
        )

        for ((key, owner), messages_by_aggregate) in itertools.groupby(
            sorted_messages,
            key=lambda m: (m.parsed_content.key, m.parsed_content.address),
        ):
            messages_by_aggregate = list(messages_by_aggregate)
            aggregate_elements = [
                await self._insert_aggregate_element(session=session, message=message)
                for message in messages_by_aggregate
            ]

            await self._update_aggregate(
                session=session, key=key, owner=owner, elements=aggregate_elements
            )

        return messages, []
