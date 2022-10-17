import itertools
from typing import Tuple, List, Dict, Iterable

from aleph.db.accessors.aggregates import get_aggregate_by_key, get_aggregate_elements, merge_aggregate_elements
from aleph.db.models import MessageDb, AggregateElementDb, AggregateDb
from aleph.handlers.content.content_handler import ContentHandler
from aleph.toolkit.split import split_iterable
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.db_session import DbSessionFactory, DbSession


class AggregateMessageHandler(ContentHandler):
    def __init__(self, session_factory: DbSessionFactory):
        self.session_factory = session_factory

    @staticmethod
    def _make_aggregate_from_elements(
        elements: Iterable[AggregateElementDb],
    ) -> Dict:
        content = {}
        for element in elements:
            content.update(element.content)
        return content

    async def _insert_aggregate_element(
        self, session: DbSession, new_element: AggregateElementDb
    ):
        aggregate = await get_aggregate_by_key(
            session=session, owner=new_element.owner, key=new_element.key
        )

        if not aggregate:
            session.add(
                AggregateDb(
                    key=new_element.key,
                    owner=new_element.owner,
                    content=new_element.content,
                    creation_datetime=new_element.creation_datetime,
                    last_revision=new_element,
                )
            )

        else:
            if (
                aggregate.last_revision.creation_datetime
                < new_element.creation_datetime
            ):
                # Insertion in order, just update the content
                aggregate.content.update(new_element.content)
                aggregate.last_revision = new_element

            else:
                # Out of order, we need to reprocess the aggregate
                older_elements, newer_elements = split_iterable(
                    await get_aggregate_elements(
                        session=session, owner=new_element.owner, key=new_element.key
                    ),
                    cond=lambda element: element.creation_datetime
                    <= new_element.creation_datetime,
                )

                elements = older_elements + [new_element] + newer_elements
                aggregate.content = merge_aggregate_elements(elements)

        session.add(new_element)

    async def fetch_related_content(
        self, session: DbSession, message: MessageDb
    ) -> None:
        return

    async def _update_aggregate(
        self, session: DbSession, key: str, owner: str, messages: Iterable[MessageDb]
    ):
        # TODO: to improve performance, only modify the aggregate once -> insert several
        #       elements at once
        for message in messages:
            new_element = AggregateElementDb(
                item_hash=message.item_hash,
                key=key,
                owner=owner,
                content=message.parsed_content.content,
                creation_datetime=timestamp_to_datetime(message.parsed_content.time),
            )
            await self._insert_aggregate_element(
                session=session, new_element=new_element
            )

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
            await self._update_aggregate(
                session=session, key=key, owner=owner, messages=messages_by_aggregate
            )

        return messages, []
