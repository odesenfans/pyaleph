import asyncio
import logging
from enum import Enum
from typing import Mapping, Literal, List, Optional, Tuple, TypeVar, Type

import aiohttp
from aleph_message.models import Chain
from configmanager import Config
from pydantic import BaseModel

from aleph.chains.connector import ChainReader
from aleph.db.accessors.chains import get_last_height
from aleph.schemas.chains.indexer_response import (
    EntityType,
    IndexerBlockchain,
    IndexerAccountStateResponse,
    IndexerEventResponse,
)
from aleph.types.chain_sync import ChainSyncType
from aleph.types.db_session import DbSessionFactory, DbSession
import datetime as dt

LOGGER = logging.getLogger(__name__)


def make_account_state_query(
    blockchain: IndexerBlockchain, accounts: List[str], type_: EntityType
):
    return """
{
  state: accountState(
    blockchain: %s
    account: %s
    type: %s
  ) {
    blockchain
    type
    indexer
    account
    accurate
    progress
    pending
    processed
  }
}
    """ % (
        blockchain,
        str(accounts),
        type_,
    )


def make_events_query(
    sync_type: ChainSyncType,
    blockchain: IndexerBlockchain,
    datetime_range: Optional[Tuple[dt.datetime, dt.datetime]] = None,
    block_range: Optional[Tuple[int], int] = None,
    limit: int = 1000,
):
    if datetime_range and block_range:
        raise ValueError("Only one range of datetimes or blocks can be specified.")
    if not datetime_range and not block_range:
        raise ValueError("A range of datetimes or blocks must be specified.")

    event_type = "messageEvents" if sync_type == ChainSyncType.MESSAGE else "syncEvents"
    params = {"blockchain": blockchain.value, "limit": limit}

    if block_range is not None:
        params["startHeight"] = block_range[0]
        params["endHeight"] = block_range[1]

    if datetime_range is not None:
        params["startDate"] = datetime_range[0].timestamp()
        params["endDate"] = datetime_range[1].timestamp()

    return """
{
  %s(%s) {
    timestamp
    address
    height
    message
  }
}
""" % (
        event_type,
        ", ".join(f"{k}: {v}" for k, v in params.items()),
    )


T = TypeVar("T", bound=BaseModel)


class AlephIndexerReader(ChainReader):

    BLOCKCHAIN_MAP: Mapping[Chain, IndexerBlockchain] = {
        Chain.ETH: IndexerBlockchain.ETHEREUM,
        Chain.SOL: IndexerBlockchain.SOLANA,
    }

    def __init__(self, chain: Chain, session_factory: DbSessionFactory):
        self.chain = chain
        self.session_factory = session_factory

    async def get_last_height(self, sync_type: ChainSyncType) -> int:
        with self.session_factory() as session:
            return get_last_height(
                session=session, chain=self.chain, sync_type=sync_type
            )

    @staticmethod
    async def _query(
        http_session: aiohttp.ClientSession, query: str, model: Type[T]
    ) -> T:
        response = await http_session.post("/", json={"query": query})
        response.raise_for_status()
        response_json = await response.json()
        return model.parse_obj(response_json)

    async def _fetch_account_state(
        self,
        http_session: aiohttp.ClientSession,
        blockchain: IndexerBlockchain,
        accounts: List[str],
    ) -> IndexerAccountStateResponse:
        query = make_account_state_query(
            blockchain=blockchain, accounts=accounts, type_=EntityType.LOG
        )

        return await self._query(
            http_session=http_session, query=query, model=IndexerAccountStateResponse
        )

    async def _fetch_events(
        self,
        http_session: aiohttp.ClientSession,
        blockchain: IndexerBlockchain,
        sync_type: ChainSyncType,
        datetime_range: Optional[Tuple[dt.datetime, dt.datetime]] = None,
        block_range: Optional[Tuple[int, int]] = None,
        limit: int = 1000,
    ) -> IndexerEventResponse:
        query = make_events_query(
            sync_type=sync_type,
            blockchain=blockchain,
            block_range=block_range,
            datetime_range=datetime_range,
            limit=limit,
        )

        return await self._query(
            http_session=http_session, query=query, model=IndexerEventResponse
        )

    async def fetch_events(
        self, session: DbSession, indexer_url: str, sync_type: ChainSyncType
    ):
        last_stored_height = await self.get_last_height(sync_type=sync_type)
        # TODO: maybe get_last_height() should not return a negative number on startup?
        # Avoid an off-by-one error at startup
        if last_stored_height == -1:
            last_stored_height = 0

        limit = 100

        try:
            while True:
                indexer_response_data = await fetch_messages(
                    http_session,
                    sync_contract_address=sync_contract_address,
                    event_type="MessageEvent",
                    limit=limit,
                    skip=last_stored_height,
                )
                txs = await extract_aleph_messages_from_indexer_response(
                    indexer_response_data
                )
                LOGGER.info("%d new txs", len(txs))
                for tx in txs:
                    await self.chain_data_service.incoming_chaindata(
                        session=session, tx=tx
                    )

                last_stored_height += limit
                if last_stored_height >= indexer_response_data.data.stats.total_events:
                    last_stored_height = indexer_response_data.data.stats.total_events
                    break

        finally:
            upsert_chain_sync_status(
                session=session,
                chain=Chain.TEZOS,
                sync_type=ChainSyncType.MESSAGE,
                height=last_stored_height,
                update_datetime=utc_now(),
            )

    async def fetch_message_events(self, session: DbSession, indexer_url: str):
        ...

    async def fetch_sync_events(self, session: DbSession, indexer_url: str):
        ...

    async def fetcher(self, config: Config):
        while True:
            try:
                with self.session_factory() as session:
                    await self.fetch_incoming_messages(
                        session=session,
                        indexer_url=config.tezos.indexer_url.value,
                        sync_contract_address=config.tezos.sync_contract.value,
                    )
                    session.commit()
            except Exception:
                LOGGER.exception(
                    "An unexpected exception occurred, "
                    "relaunching Tezos message sync in 10 seconds"
                )
            else:
                LOGGER.info("Processed all transactions, waiting 10 seconds.")
            await asyncio.sleep(10)
