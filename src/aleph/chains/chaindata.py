import asyncio
import json
from typing import Dict, Optional, List

from aleph_message.models import Chain
from sqlalchemy.ext.asyncio import AsyncSession

from aleph.chains.common import LOGGER
from aleph.chains.tx_context import TxContext
from aleph.config import get_config
from aleph.db.models import ChainTxDb
from aleph.db.models.file_pins import FilePinDb
from aleph.db.models.pending_txs import ChainSyncProtocol, PendingTxDb
from aleph.exceptions import (
    InvalidContent,
    AlephStorageException,
    ContentCurrentlyUnavailable,
)
from aleph.storage import StorageService
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.db_session import DbSessionFactory


class ChainDataService:
    def __init__(
        self, session_factory: DbSessionFactory, storage_service: StorageService
    ):
        self.session_factory = session_factory
        self.storage_service = storage_service

    async def get_chaindata(self, messages, bulk_threshold: int = 2000):
        """Returns content ready to be broadcasted on-chain (aka chaindata).

        If message length is over bulk_threshold (default 2000 chars), store list
        in IPFS and store the object hash instead of raw list.
        """
        chaindata = {
            "protocol": ChainSyncProtocol.OnChain,
            "version": 1,
            "content": {"messages": messages},
        }
        content = json.dumps(chaindata)
        if len(content) > bulk_threshold:
            ipfs_id = await self.storage_service.add_json(chaindata)
            return json.dumps(
                {
                    "protocol": ChainSyncProtocol.OffChain,
                    "version": 1,
                    "content": ipfs_id,
                }
            )
        else:
            return content

    async def get_chaindata_messages(
        self, chaindata: Dict, context: TxContext, seen_ids: Optional[List[str]] = None
    ):
        config = get_config()

        protocol = chaindata.get("protocol", None)
        version = chaindata.get("version", None)
        if protocol == "aleph" and version == 1:
            messages = chaindata["content"]["messages"]
            if not isinstance(messages, list):
                error_msg = f"Got bad data in tx {context!r}"
                raise InvalidContent(error_msg)
            return messages

        if protocol == "aleph-offchain" and version == 1:
            assert isinstance(chaindata.get("content"), str)
            if seen_ids is not None:
                if chaindata["content"] in seen_ids:
                    # is it really what we want here?
                    LOGGER.debug("Already seen")
                    return None
                else:
                    LOGGER.debug("Adding to seen_ids")
                    seen_ids.append(chaindata["content"])
            try:
                content = await self.storage_service.get_json(
                    chaindata["content"], timeout=60
                )
            except AlephStorageException:
                # Let the caller handle unavailable/invalid content
                raise
            except Exception as e:
                error_msg = (
                    f"Can't get content of offchain object {chaindata['content']!r}"
                )
                LOGGER.exception("%s", error_msg)
                raise ContentCurrentlyUnavailable(error_msg) from e

            try:
                messages = await self.get_chaindata_messages(content.value, context)
            except AlephStorageException:
                LOGGER.debug("Got no message")
                raise

            LOGGER.info("Got bulk data with %d items" % len(messages))
            if config.ipfs.enabled.value:
                try:
                    LOGGER.info(f"chaindata {chaindata}")
                    async with self.session_factory() as session:
                        session.add(
                            FilePinDb(
                                file_hash=chaindata["content"], tx_hash=context.tx_hash
                            )
                        )
                        await session.commit()

                    # Some IPFS fetches can take a while, hence the large timeout.
                    await asyncio.wait_for(
                        self.storage_service.pin_hash(chaindata["content"]), timeout=120
                    )
                except asyncio.TimeoutError:
                    LOGGER.warning(f"Can't pin hash {chaindata['content']}")
            return messages
        else:
            error_msg = f"Got unknown protocol/version object in tx {context!r}"
            LOGGER.info("%s", error_msg)
            raise InvalidContent(error_msg)

    @staticmethod
    async def incoming_chaindata(
        session: AsyncSession, content: Dict, context: TxContext
    ):
        """Incoming data from a chain.
        Content can be inline of "offchain" through an ipfs hash.
        For now we only add it to the database, it will be processed later.
        """
        session.add(
            PendingTxDb(
                protocol=content["protocol"],
                protocol_version=content["version"],
                content=content["content"],
                tx=ChainTxDb(
                    hash=context.tx_hash,
                    chain=Chain(context.chain_name),
                    height=context.height,
                    datetime=timestamp_to_datetime(context.time),
                    publisher=context.publisher,
                ),
            )
        )
