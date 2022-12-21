""" This is the storage message handlers file.

For now it's very simple, we check if we want to store files or not.

TODO:
- check balances and storage allowance
- handle incentives from 3rd party
- handle garbage collection of unused hashes
"""

import asyncio
import logging
from typing import List, Tuple, Optional

import aioipfs
from aleph_message.models import ItemType, StoreContent, ItemHash

from aleph.config import get_config
from aleph.db.accessors.files import (
    make_upsert_stored_file_query,
    insert_message_file_pin,
    get_file_tag,
    upsert_file_tag,
)
from aleph.db.models import MessageDb, StoredFileDb
from aleph.exceptions import AlephStorageException, UnknownHashError
from aleph.handlers.content.content_handler import ContentHandler
from aleph.schemas.validated_message import (
    StoreContentWithMetadata,
    EngineInfo,
)
from aleph.storage import StorageService
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.db_session import DbSessionFactory, DbSession
from aleph.types.files import FileTag, FileType
from aleph.types.message_status import (
    MessageUnavailable,
    InvalidMessageException,
    PermissionDenied,
)
from aleph.utils import item_type_from_hash

LOGGER = logging.getLogger(__name__)


def _get_store_content(message: MessageDb) -> StoreContent:
    content = message.parsed_content
    if not isinstance(content, StoreContent):
        raise InvalidMessageException(
            f"Unexpected content type for store message: {message.item_hash}"
        )
    return content


def make_file_tag(owner: str, ref: Optional[str], item_hash: str) -> FileTag:
    """
    Builds the file tag corresponding to a STORE message.

    The file tag can be set to two different values:
    * if the `ref` field is not set, the tag will be set to <item_hash>.
    * if the `ref` field is set, two cases: if `ref` is an item hash, the tag is
      the value of the ref field. If it is a user-defined value, the tag is
      <owner>/<ref>.

    :param owner: Owner of the file.
    :param ref: Value of the `ref` field of the message content.
    :param item_hash: Item hash of the message.
    :return: The computed file tag.
    """

    # When the user does not specify a ref, we use the item hash.
    if ref is None:
        return FileTag(item_hash)

    # If ref is an item hash, return it as is
    try:
        _item_hash = ItemHash(ref)
        return FileTag(ref)
    except ValueError:
        pass

    return FileTag(f"{owner}/{ref}")


class StoreMessageHandler(ContentHandler):
    def __init__(
        self, session_factory: DbSessionFactory, storage_service: StorageService
    ):
        self.session_factory = session_factory
        self.storage_service = storage_service

    async def is_related_content_fetched(
        self, session: DbSession, message: MessageDb
    ) -> bool:
        content = message.parsed_content
        assert isinstance(content, StoreContent)

        file_hash = content.item_hash
        return await self.storage_service.storage_engine.exists(file_hash)

    async def fetch_related_content(
        self, session: DbSession, message: MessageDb
    ) -> None:

        # TODO: this check is useless, remove it
        config = get_config()
        if not config.storage.store_files.value:
            return  # Ignore

        content = message.parsed_content
        assert isinstance(content, StoreContent)

        engine = content.item_type
        output_content = StoreContentWithMetadata.from_content(content)

        is_folder = False
        item_hash = content.item_hash

        ipfs_enabled = config.ipfs.enabled.value
        do_standard_lookup = True
        size = None

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
                    raise MessageUnavailable(
                        "Could not retrieve IPFS content at this time"
                    )

                if (
                    stats["Type"] == "file"
                    and stats["CumulativeSize"] < 1024 ** 2
                    and len(item_hash) == 46
                ):
                    do_standard_lookup = True
                else:
                    size = stats["CumulativeSize"]
                    output_content.engine_info = EngineInfo(**stats)
                    timer = 0
                    is_folder = stats["Type"] == "directory"
                    async for status in ipfs_client.pin.add(item_hash):
                        timer += 1
                        if timer > 30 and "Pins" not in status:
                            raise MessageUnavailable(
                                "Could not pin IPFS content at this time"
                            )
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
                raise MessageUnavailable(
                    "Could not retrieve file from storage at this time"
                )

            size = len(file_content)

        stored_file = StoredFileDb(
            hash=item_hash,
            type=FileType.DIRECTORY if is_folder else FileType.FILE,
            size=size,
        )
        session.execute(make_upsert_stored_file_query(stored_file))

    async def check_permissions(self, session: DbSession, message: MessageDb):
        content = _get_store_content(message)
        if content.ref is None:
            return

        owner = content.address
        file_tag = make_file_tag(owner=owner, ref=content.ref, item_hash=message.item_hash)
        file_tag_db = await get_file_tag(session=session, tag=file_tag)

        if not file_tag_db:
            return

        if file_tag_db.owner != owner:
            raise PermissionDenied(
                f"{message.item_hash} attempts to update a file tag belonging to another user"
            )

    async def _pin_and_tag_file(self, session: DbSession, message: MessageDb):
        content = _get_store_content(message)

        file_hash = content.item_hash
        owner = content.address

        await insert_message_file_pin(
            session=session,
            file_hash=file_hash,
            owner=owner,
            item_hash=message.item_hash,
            ref=content.ref,
            created=timestamp_to_datetime(content.time),
        )

        file_tag = make_file_tag(
            owner=content.address, ref=content.ref, item_hash=message.item_hash
        )
        await upsert_file_tag(
            session=session,
            tag=file_tag,
            owner=owner,
            file_hash=file_hash,
            last_updated=timestamp_to_datetime(content.time),
        )

    async def process(
        self, session: DbSession, messages: List[MessageDb]
    ) -> Tuple[List[MessageDb], List[MessageDb]]:
        for message in messages:
            await self._pin_and_tag_file(session=session, message=message)

        return messages, []
