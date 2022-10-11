import asyncio
import datetime as dt
import functools
import json
import logging
from typing import AsyncIterator, Dict, Tuple

import pkg_resources
from aleph_message.models import Chain
from configmanager import Config
from eth_account import Account
from eth_account.messages import encode_defunct
from hexbytes import HexBytes
from web3 import Web3
from web3._utils.events import get_event_data
from web3.gas_strategies.rpc import rpc_gas_price_strategy
from web3.middleware.filter import local_filter_middleware
from web3.middleware.geth_poa import geth_poa_middleware

from aleph.chains.common import get_verification_buffer
from aleph.db.accessors.chains import get_last_height, upsert_chain_sync_status
from aleph.schemas.pending_messages import BasePendingMessage
from aleph.types.db_session import DbSessionFactory
from aleph.utils import run_in_executor
from .chaindata import ChainDataService
from .connector import ChainWriter, Verifier
from .tx_context import TxContext
from ..db.accessors.messages import get_unconfirmed_messages
from ..db.accessors.pending_messages import count_pending_messages
from ..db.accessors.pending_txs import count_pending_txs

LOGGER = logging.getLogger("chains.ethereum")
CHAIN_NAME = "ETH"


def get_web3(config) -> Web3:
    web3 = Web3(Web3.HTTPProvider(config.ethereum.api_url.value))
    if config.ethereum.chain_id.value == 4:  # rinkeby
        web3.middleware_onion.inject(geth_poa_middleware, layer=0)
    web3.middleware_onion.add(local_filter_middleware)
    web3.eth.setGasPriceStrategy(rpc_gas_price_strategy)

    return web3


async def get_contract_abi():
    return json.loads(
        pkg_resources.resource_string(
            "aleph.chains", "assets/ethereum_sc_abi.json"
        ).decode("utf-8")
    )


async def get_contract(config, web3: Web3):
    return web3.eth.contract(
        config.ethereum.sync_contract.value, abi=await get_contract_abi()
    )


async def get_logs_query(web3: Web3, contract, start_height, end_height):
    logs = await run_in_executor(
        None,
        web3.eth.getLogs,
        {"address": contract.address, "fromBlock": start_height, "toBlock": end_height},
    )
    for log in logs:
        yield log


class EthereumConnector(Verifier, ChainWriter):
    def __init__(
        self, session_factory: DbSessionFactory, chain_data_service: ChainDataService
    ):
        self.session_factory = session_factory
        self.chain_data_service = chain_data_service

    async def verify_signature(self, message: BasePendingMessage) -> bool:
        """Verifies a signature of a message, return True if verified, false if not"""

        verification = get_verification_buffer(message)

        message_hash = await run_in_executor(
            None, functools.partial(encode_defunct, text=verification.decode("utf-8"))
        )
        verified = False
        try:
            # we assume the signature is a valid string
            address = await run_in_executor(
                None,
                functools.partial(
                    Account.recover_message, message_hash, signature=message.signature
                ),
            )
            if address == message.sender:
                verified = True
            else:
                LOGGER.warning(
                    "Received bad signature from %s for %s" % (address, message.sender)
                )
                return False

        except Exception as e:
            LOGGER.exception("Error processing signature for %s" % message.sender)
            verified = False

        return verified

    async def get_last_height(self) -> int:
        """Returns the last height for which we already have the ethereum data."""
        async with self.session_factory() as session:
            last_height = await get_last_height(session=session, chain=Chain.ETH)

        if last_height is None:
            last_height = -1

        return last_height

    @staticmethod
    async def _get_logs(config, web3: Web3, contract, start_height):
        try:
            logs = get_logs_query(web3, contract, start_height + 1, "latest")
            async for log in logs:
                yield log
        except ValueError as e:
            # we got an error, let's try the pagination aware version.
            if e.args[0]["code"] != -32005:
                return

            last_block = await asyncio.get_event_loop().run_in_executor(
                None, web3.eth.get_block_number
            )
            if start_height < config.ethereum.start_height.value:
                start_height = config.ethereum.start_height.value

            end_height = start_height + 1000

            while True:
                try:
                    logs = get_logs_query(web3, contract, start_height, end_height)
                    async for log in logs:
                        yield log

                    start_height = end_height + 1
                    end_height = start_height + 1000

                    if start_height > last_block:
                        LOGGER.info("Ending big batch sync")
                        break

                except ValueError as e:
                    if e.args[0]["code"] == -32005:
                        end_height = start_height + 100
                    else:
                        raise

    async def _request_transactions(
        self, config, web3: Web3, contract, abi, start_height
    ) -> AsyncIterator[Tuple[Dict, TxContext]]:
        """Continuously request data from the Ethereum blockchain.
        TODO: support websocket API.
        """

        logs = self._get_logs(config, web3, contract, start_height + 1)

        async for log in logs:
            event_data = await run_in_executor(
                None, get_event_data, web3.codec, abi, log
            )
            LOGGER.info("Handling TX in block %s" % event_data.blockNumber)
            publisher = event_data.args.addr
            timestamp = event_data.args.timestamp

            if publisher not in config.ethereum.authorized_emitters.value:
                LOGGER.info(
                    "TX with unauthorized emitter %s in block %s"
                    % (publisher, event_data.blockNumber)
                )
                continue

            last_height = event_data.blockNumber

            message = event_data.args.message
            try:
                jdata = json.loads(message)
                context = TxContext(
                    chain_name=CHAIN_NAME,
                    tx_hash=event_data.transactionHash.hex(),
                    time=timestamp,
                    height=event_data.blockNumber,
                    publisher=publisher,
                )
                yield jdata, context

            except json.JSONDecodeError:
                # if it's not valid json, just ignore it...
                LOGGER.info("Incoming logic data is not JSON, ignoring. %r" % message)

            except Exception:
                LOGGER.exception("Can't decode incoming logic data %r" % message)

            # Since we got no critical exception, save last received object
            # block height to do next requests from there.
            if last_height:
                async with self.session_factory() as session:
                    await upsert_chain_sync_status(
                        session=session,
                        chain=Chain.ETH,
                        height=last_height,
                        update_datetime=dt.datetime.utcnow(),
                    )
                    await session.commit()

    async def fetcher(self, config: Config):
        last_stored_height = await self.get_last_height()

        LOGGER.info("Last block is #%d" % last_stored_height)

        web3 = await run_in_executor(None, get_web3, config)
        contract = await get_contract(config, web3)
        abi = contract.events.SyncEvent._get_event_abi()

        while True:
            last_stored_height = await self.get_last_height()
            async for jdata, context in self._request_transactions(
                config, web3, contract, abi, last_stored_height
            ):
                async with self.session_factory() as session:
                    await self.chain_data_service.incoming_chaindata(
                        session=session, content=jdata, context=context
                    )
                    await session.commit()

    @staticmethod
    def _broadcast_content(
        config, contract, web3: Web3, account, gas_price, nonce, content
    ):
        tx = contract.functions.doEmit(content).buildTransaction(
            {
                "chainId": config.ethereum.chain_id.value,
                "gasPrice": gas_price,
                "nonce": nonce,
            }
        )
        signed_tx = account.signTransaction(tx)
        return web3.eth.sendRawTransaction(signed_tx.rawTransaction)

    async def packer(self, config: Config):
        web3 = await run_in_executor(None, get_web3, config)
        contract = await get_contract(config, web3)

        pri_key = HexBytes(config.ethereum.private_key.value)
        account = Account.privateKeyToAccount(pri_key)
        address = account.address

        LOGGER.info("Ethereum Connector set up with address %s" % address)
        i = 0
        gas_price = web3.eth.generateGasPrice()
        while True:
            async with self.session_factory() as session:

                # Wait for sync operations to complete
                if (await count_pending_txs(session=session, chain=Chain.ETH)) or (
                    await count_pending_messages(session=session, chain=Chain.ETH)
                ) > 1000:
                    await asyncio.sleep(30)
                    continue
                gas_price = web3.eth.generateGasPrice()

                if i >= 100:
                    await asyncio.sleep(30)  # wait three (!!) blocks
                    gas_price = web3.eth.generateGasPrice()
                    i = 0

                if gas_price > config.ethereum.max_gas_price.value:
                    # gas price too high, wait a bit and retry.
                    await asyncio.sleep(60)
                    continue

                nonce = web3.eth.getTransactionCount(account.address)

                messages = list(
                    await get_unconfirmed_messages(
                        session=session, limit=10000, chain=Chain.ETH
                    )
                )

            if len(messages):
                content = await self.chain_data_service.get_chaindata(
                    messages, bulk_threshold=200
                )
                response = await run_in_executor(
                    None,
                    self._broadcast_content,
                    config,
                    contract,
                    web3,
                    account,
                    int(gas_price * 1.1),
                    nonce,
                    content,
                )
                LOGGER.info("Broadcasted %r on %s" % (response, CHAIN_NAME))

            await asyncio.sleep(config.ethereum.commit_delay.value)
            i += 1
