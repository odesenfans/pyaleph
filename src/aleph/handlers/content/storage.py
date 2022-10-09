""" This is the storage message handlers file.

For now it's very simple, we check if we want to store files or not.

TODO:
- check balances and storage allowance
- handle incentives from 3rd party
- handle garbage collection of unused hashes
"""

import asyncio
import logging
from typing import List, Tuple

import aioipfs
from aleph_message.models import ItemType, StoreContent

from aleph.config import get_config
from aleph.db.accessors.files import upsert_stored_file, make_upsert_stored_file_query
from aleph.db.bulk_operations import DbBulkOperation
from aleph.db.models import MessageDb, StoredFileDb, FileReferenceDb
from aleph.exceptions import AlephStorageException, UnknownHashError
from aleph.handlers.content.content_handler import ContentHandler
from aleph.schemas.validated_message import (
    StoreContentWithMetadata,
    EngineInfo,
)
from aleph.storage import StorageService
from aleph.types.db_session import DbSessionFactory, DbSession
from aleph.types.file_type import FileType
from aleph.types.message_status import MessageProcessingStatus
from aleph.utils import item_type_from_hash

LOGGER = logging.getLogger("HANDLERS.STORAGE")


class StoreMessageHandler(ContentHandler):
    def __init__(
        self, session_factory: DbSessionFactory, storage_service: StorageService
    ):
        self.session_factory = session_factory
        self.storage_service = storage_service

    async def fetch_related_content(
        self, session: DbSession, message: MessageDb
    ) -> Tuple[MessageProcessingStatus, List[DbBulkOperation]]:
        config = get_config()
        if not config.storage.store_files.value:
            return MessageProcessingStatus.MESSAGE_HANDLED, []  # Ignore

        content = message.parsed_content
        assert isinstance(content, StoreContent)

        engine = content.item_type
        output_content = StoreContentWithMetadata.from_content(content)

        is_folder = False
        item_hash = content.item_hash

        ipfs_enabled = config.ipfs.enabled.value
        do_standard_lookup = True

        if engine == ItemType.ipfs and ipfs_enabled:
            if item_type_from_hash(item_hash) != ItemType.ipfs:
                LOGGER.warning("Invalid IPFS hash: '%s'", item_hash)
                raise UnknownHashError(f"Invalid IPFS hash: '{item_hash}'")

            ipfs_client = self.storage_service.ipfs_service.ipfs_client

            try:
                try:
                    stats = await asyncio.wait_for(
                        ipfs_client.files.stat(f"/ipfs/{item_hash}"), 5
                    )
                except aioipfs.InvalidCIDError as e:
                    raise UnknownHashError(
                        f"Invalid IPFS hash from API: '{item_hash}'"
                    ) from e
                if stats is None:
                    return MessageProcessingStatus.RETRYING_LATER, []

                if (
                    stats["Type"] == "file"
                    and stats["CumulativeSize"] < 1024**2
                    and len(item_hash) == 46
                ):
                    do_standard_lookup = True
                else:
                    output_content.size = stats["CumulativeSize"]
                    output_content.engine_info = EngineInfo(**stats)
                    timer = 0
                    is_folder = stats["Type"] == "directory"
                    async for status in ipfs_client.pin.add(item_hash):
                        timer += 1
                        if timer > 30 and "Pins" not in status:
                            return (
                                MessageProcessingStatus.RETRYING_LATER,
                                [],
                            )  # Can't retrieve data now.
                    do_standard_lookup = False

            except asyncio.TimeoutError as error:
                LOGGER.warning(
                    f"Timeout while retrieving stats of hash {item_hash}: {getattr(error, 'message', None)}"
                )
                do_standard_lookup = True

            except aioipfs.APIError as error:
                LOGGER.exception(
                    f"Error retrieving stats of hash {item_hash}: {getattr(error, 'message', None)}"
                )
                do_standard_lookup = True

        if do_standard_lookup:
            # TODO: We should check the balance here.
            try:
                file_content = await self.storage_service.get_hash_content(
                    item_hash,
                    engine=engine,
                    tries=4,
                    timeout=2,
                    use_network=True,
                    use_ipfs=True,
                    store_value=True,
                )
            except AlephStorageException:
                return MessageProcessingStatus.RETRYING_LATER, []

            output_content.size = len(file_content)

        stored_file = StoredFileDb(
            hash=item_hash, type=FileType.DIRECTORY if is_folder else FileType.FILE
        )
        ops = [make_upsert_stored_file_query(stored_file)]
        # store_message.content = output_content

        return MessageProcessingStatus.MESSAGE_HANDLED, ops

    async def _create_file_reference(self, session: DbSession, message: MessageDb):
        content = message.parsed_content
        assert isinstance(content, StoreContent)

        session.add(
            FileReferenceDb(
                file_hash=content.item_hash,
                owner=content.address,
                item_hash=message.item_hash,
            )
        )

    async def process(self, messages: List[MessageDb]) -> MessageProcessingStatus:
        async with self.session_factory() as session:
            for message in messages:
                await self._create_file_reference(session=session, message=message)

        return MessageProcessingStatus.MESSAGE_HANDLED
