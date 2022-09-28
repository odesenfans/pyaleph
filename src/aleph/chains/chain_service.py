import asyncio
import logging
from typing import Dict

from aleph_message.models import Chain
from configmanager import Config

from aleph.schemas.pending_messages import BasePendingMessage
from .connector import ChainConnector, ChainReader, ChainWriter, Verifier
from ..exceptions import InvalidMessageError

LOGGER = logging.getLogger(__name__)


class ChainService:
    connectors: Dict[Chain, ChainConnector]
    verifiers: Dict[Chain, Verifier]
    readers: Dict[Chain, ChainReader]
    writers: Dict[Chain, ChainWriter]

    def __init__(self):

        self.connectors = {}
        self.verifiers = {}
        self.readers = {}
        self.writers = {}

        self._register_chains()

    async def verify_signature(self, message: BasePendingMessage) -> None:
        try:
            verifier = self.verifiers[message.chain]
        except KeyError:
            raise InvalidMessageError(f"Unknown chain for validation: {message.chain}")

        try:
            if await verifier.verify_signature(message):
                return
            else:
                raise InvalidMessageError("The signature of the message is invalid")

        except ValueError:
            raise InvalidMessageError("Signature validation error")

    async def chain_reader_task(self, chain: Chain, config: Config):
        connector = self.readers[chain]

        while True:
            try:
                await connector.fetcher(config)
            except Exception:
                LOGGER.exception(
                    "Chain reader task for %s failed, retrying in 10 seconds.", chain
                )

            await asyncio.sleep(10)

    async def chain_writer_task(self, chain: Chain, config: Config):
        connector = self.writers[chain]

        while True:
            try:
                await connector.packer(config)
            except Exception:
                LOGGER.exception(
                    "Chain writer task for %s failed, relaunching in 10 seconds.", chain
                )
                await asyncio.sleep(10)

    async def chain_event_loop(self, config: Config):
        listener_tasks = []
        publisher_tasks = []

        if config.ethereum.enabled.value:
            listener_tasks.append(self.chain_reader_task(Chain.ETH, config))
            if config.ethereum.packing_node.value:
                publisher_tasks.append(self.chain_writer_task(Chain.ETH, config))

        await asyncio.gather(*(listener_tasks + publisher_tasks))

    def _add_chain(self, chain: Chain, connector: ChainConnector):
        self.connectors[chain] = connector

        if isinstance(connector, Verifier):
            self.verifiers[chain] = connector
        if isinstance(connector, ChainReader):
            self.readers[chain] = connector
        if isinstance(connector, ChainWriter):
            self.writers[chain] = connector

    def _register_chains(self):
        try:
            from .avalanche import AvalancheConnector

            self._add_chain(Chain.AVAX, AvalancheConnector())
        except ModuleNotFoundError as error:
            LOGGER.warning("Can't load AVAX: %s", error.msg)
        try:
            from .nuls import NulsConnector

            self._add_chain(Chain.NULS, NulsConnector())
        except ModuleNotFoundError as error:
            LOGGER.warning("Can't load NULS: %s", error.msg)
        try:
            from .nuls2 import Nuls2Connector

            self._add_chain(Chain.NULS2, Nuls2Connector())
        except ModuleNotFoundError as error:
            LOGGER.warning("Can't load NULS2: %s", error.msg)
        try:
            from .ethereum import EthereumConnector

            self._add_chain(Chain.ETH, EthereumConnector())
        except ModuleNotFoundError as error:
            LOGGER.warning("Can't load ETH: %s", error.msg)
        try:
            from .substrate import SubstrateConnector

            self._add_chain(Chain.DOT, SubstrateConnector())
        except (ModuleNotFoundError, ImportError) as error:
            LOGGER.warning("Can't load DOT: %s", error.msg)
        try:
            from .cosmos import CosmosConnector

            self._add_chain(Chain.CSDK, CosmosConnector())
        except ModuleNotFoundError as error:
            LOGGER.warning("Can't load CSDK: %s", error.msg)
        try:
            from .solana import SolanaConnector

            self._add_chain(Chain.SOL, SolanaConnector())
        except ModuleNotFoundError as error:
            LOGGER.warning("Can't load SOL: %s", error.msg)
        try:
            from .tezos import TezosConnector

            self._add_chain(Chain.TEZOS, TezosConnector())
        except ModuleNotFoundError as error:
            LOGGER.warning("Can't load Tezos: %s", error.msg)
