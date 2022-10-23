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
    update_aggregate, count_aggregate_elements, mark_aggregate_as_dirty,
)
from aleph.db.models import MessageDb, AggregateElementDb, AggregateDb
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
        # Nothing to do, aggregates are independent of one another
        return

    @staticmethod
    async def _insert_aggregate_element(session: DbSession, message: MessageDb):
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
    async def _append_to_aggregate(
        session: DbSession,
        aggregate: AggregateDb,
        elements: Sequence[AggregateElementDb],
    ):
        new_content = merge_aggregate_elements(elements)
        aggregate.content.update(new_content)

        await update_aggregate(
            session=session,
            key=aggregate.key,
            owner=aggregate.owner,
            content=aggregate.content,
            last_revision_hash=elements[-1].item_hash,
            creation_datetime=aggregate.creation_datetime,
        )

    @staticmethod
    async def _prepend_to_aggregate(
        session: DbSession,
        aggregate: AggregateDb,
        elements: Sequence[AggregateElementDb],
    ):
        new_content = merge_aggregate_elements(elements)
        new_content.update(aggregate.content)

        await update_aggregate(
            session=session,
            key=aggregate.key,
            owner=aggregate.owner,
            content=new_content,
            last_revision_hash=aggregate.last_revision_hash,
            creation_datetime=elements[0].creation_datetime,
        )

    async def _update_aggregate(
        self,
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
        dirty_threshold = 5000

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

        if aggregate.dirty:
            LOGGER.info("%s/%s is dirty, skipping update", owner, key)
            return

        LOGGER.info("%s/%s already exists, updating it", owner, key)

        # Best case scenario: the elements we are adding are all posterior to the last
        # update, we can just merge the content of aggregate and the new elements.
        if aggregate.last_revision.creation_datetime < elements[0].creation_datetime:
            await self._append_to_aggregate(
                session=session, aggregate=aggregate, elements=elements
            )
            return

        # Similar case, all the new elements are anterior to the aggregate.
        if aggregate.creation_datetime > elements[-1].creation_datetime:
            await self._prepend_to_aggregate(
                session=session, aggregate=aggregate, elements=elements
            )
            return

        LOGGER.info("%s/%s: out of order refresh", owner, key)
        if await count_aggregate_elements(session=session, owner=owner, key=key) > dirty_threshold:
            LOGGER.info("%s/%s: too many elements, marking as dirty")
            await mark_aggregate_as_dirty(session=session, owner=owner, key=key)
            return

        # Out of order insertions. Here, we need to get all the elements in the database
        # and recompute the aggregate entirely. This operation may be quite costly for
        # large aggregates, so we do it as a last resort.
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
            aggregate_elements = [
                await self._insert_aggregate_element(session=session, message=message)
                for message in messages_by_aggregate
            ]

            await self._update_aggregate(
                session=session, key=key, owner=owner, elements=aggregate_elements
            )

        return messages, []
