import asyncio
import base64
import datetime as dt
import functools
import json
import logging
import struct
import time
from operator import itemgetter
from typing import AsyncIterator, Dict, Tuple

import aiohttp
from aleph_message.models import Chain
from coincurve.keys import PrivateKey
from configmanager import Config
from nuls2.api.server import get_server
from nuls2.model.data import (
    hash_from_address,
    recover_message_address,
    get_address,
    CHEAP_UNIT_FEE,
)
from nuls2.model.transaction import Transaction
from sqlalchemy.orm import sessionmaker

from aleph.chains.common import get_verification_buffer
from aleph.model.messages import Message
from aleph.model.pending import pending_messages_count, pending_txs_count
from aleph.utils import run_in_executor
from .chaindata import ChainDataService
from .connector import Verifier, ChainWriter
from .tx_context import TxContext
from ..db.accessors.chains import get_last_height, upsert_chain_sync_status
from ..schemas.pending_messages import BasePendingMessage

LOGGER = logging.getLogger("chains.nuls2")
CHAIN_NAME = "NULS2"
PAGINATION = 500

DECIMALS = None  # will get populated later... bad?


class Nuls2Connector(Verifier, ChainWriter):
    def __init__(
        self, session_factory: sessionmaker, chain_data_service: ChainDataService
    ):
        self.session_factory = session_factory
        self.chain_data_service = chain_data_service

    async def verify_signature(self, message: BasePendingMessage) -> bool:
        """Verifies a signature of a message, return True if verified, false if not"""
        sig_raw = base64.b64decode(message.signature)

        sender_hash = hash_from_address(message.sender)
        (sender_chain_id,) = struct.unpack("h", sender_hash[:2])
        verification = get_verification_buffer(message)
        try:
            address = await run_in_executor(
                None,
                functools.partial(
                    recover_message_address,
                    sig_raw,
                    verification,
                    chain_id=sender_chain_id,
                ),
            )
        except Exception:
            LOGGER.exception("NULS Signature verification error")
            return False

        if address != message.sender:
            LOGGER.warning(
                "Received bad signature from %s for %s" % (address, message.sender)
            )
            return False
        else:
            return True

    async def get_last_height(self) -> int:
        """Returns the last height for which we already have the nuls data."""
        async with self.session_factory() as session:
            last_height = await get_last_height(session=session, chain=Chain.NULS2)

        if last_height is None:
            last_height = -1

        return last_height

    async def _request_transactions(
        self, config, session, start_height
    ) -> AsyncIterator[Tuple[Dict, TxContext]]:
        """Continuously request data from the NULS blockchain."""
        target_addr = config.nuls2.sync_address.value
        remark = config.nuls2.remark.value
        chain_id = config.nuls2.chain_id.value

        last_height = None
        async for tx in get_transactions(
            config, session, chain_id, target_addr, start_height, remark=remark
        ):
            ldata = tx["txDataHex"]
            LOGGER.info("Handling TX in block %s" % tx["height"])
            try:
                ddata = bytes.fromhex(ldata).decode("utf-8")
                last_height = tx["height"]
                jdata = json.loads(ddata)

                context = TxContext(
                    chain_name=CHAIN_NAME,
                    tx_hash=tx["hash"],
                    height=tx["height"],
                    time=tx["createTime"],
                    publisher=tx["coinFroms"][0]["address"],
                )
                yield jdata, context

            except json.JSONDecodeError:
                # if it's not valid json, just ignore it...
                LOGGER.info("Incoming logic data is not JSON, ignoring. %r" % ldata)

        if last_height:
            async with self.session_factory() as session:
                await upsert_chain_sync_status(
                    session=session,
                    chain=Chain.NULS2,
                    height=last_height,
                    update_datetime=dt.datetime.utcnow(),
                )

    async def fetcher(self, config: Config):
        last_stored_height = await self.get_last_height()

        LOGGER.info("Last block is #%d" % last_stored_height)
        async with aiohttp.ClientSession() as session:
            while True:
                last_stored_height = await self.get_last_height()
                async for jdata, context in self._request_transactions(
                    config, session, last_stored_height + 1
                ):
                    async with self.session_factory() as session:
                        await self.chain_data_service.incoming_chaindata(
                            session=session, content=jdata, context=context
                        )
                        await session.commit()

                await asyncio.sleep(10)

    async def packer(self, config: Config):
        server = get_server(config.nuls2.api_url.value)
        target_addr = config.nuls2.sync_address.value
        remark = config.nuls2.remark.value.encode("utf-8")

        pri_key = bytes.fromhex(config.nuls2.private_key.value)
        privkey = PrivateKey(pri_key)
        pub_key = privkey.public_key.format()
        chain_id = config.nuls2.chain_id.value
        address = get_address(pub_key, config.nuls2.chain_id.value)

        LOGGER.info("NULS2 Connector set up with address %s" % address)
        i = 0
        nonce = await get_nonce(server, address, chain_id)

        while True:
            if (await pending_txs_count(chain=CHAIN_NAME)) or (
                await pending_messages_count(source_chain=CHAIN_NAME)
            ):
                await asyncio.sleep(30)
                continue

            if i >= 100:
                await asyncio.sleep(30)  # wait three (!!) blocks
                nonce = await get_nonce(server, address, chain_id)
                i = 0

            messages = [
                message
                async for message in (
                    await Message.get_unconfirmed_raw(limit=10000, for_chain=CHAIN_NAME)
                )
            ]

            if len(messages):
                content = await self.chain_data_service.get_chaindata(messages)

                tx = await prepare_transfer_tx(
                    address,
                    [(target_addr, CHEAP_UNIT_FEE)],
                    nonce,
                    chain_id=chain_id,
                    asset_id=1,
                    raw_tx_data=content.encode("utf-8"),
                    remark=remark,
                )
                await tx.sign_tx(pri_key)
                tx_hex = (await tx.serialize(update_data=False)).hex()
                ret = await broadcast(server, tx_hex, chain_id=chain_id)
                LOGGER.info("Broadcasted %r on %s" % (ret["hash"], CHAIN_NAME))
                nonce = ret["hash"][-16:]

            await asyncio.sleep(config.nuls2.commit_delay.value)
            i += 1


async def get_base_url(config):
    return config.nuls2.explorer_url.value


async def get_transactions(
    config, session, chain_id, target_addr, start_height, end_height=None, remark=None
):
    check_url = "{}transactions.json".format(await get_base_url(config))

    async with session.get(
        check_url,
        params={
            "address": target_addr,
            "sort_order": 1,
            "startHeight": start_height + 1,
            "pagination": 500,
        },
    ) as resp:
        jres = await resp.json()
        for tx in sorted(jres["transactions"], key=itemgetter("height")):
            if remark is not None and tx["remark"] != remark:
                continue

            yield tx


async def broadcast(server, tx_hex, chain_id=1):
    return await server.broadcastTx(chain_id, tx_hex)


async def get_balance(server, address, chain_id, asset_id):
    return await server.getAccountBalance(chain_id, chain_id, asset_id, address)


async def prepare_transfer_tx(
    address, targets, nonce, chain_id=1, asset_id=1, remark=b"", raw_tx_data=None
):
    """Targets are tuples: address and value."""
    outputs = [
        {
            "address": add,
            "amount": val,
            "lockTime": 0,
            "assetsChainId": chain_id,
            "assetsId": asset_id,
        }
        for add, val in targets
    ]

    tx = await Transaction.from_dict(
        {
            "type": 2,
            "time": int(time.time()),
            "remark": remark,
            "coinFroms": [
                {
                    "address": address,
                    "assetsChainId": chain_id,
                    "assetsId": asset_id,
                    "amount": 0,
                    "nonce": nonce,
                    "locked": 0,
                }
            ],
            "coinTos": outputs,
        }
    )
    tx.inputs[0]["amount"] = (await tx.calculate_fee()) + sum(
        [o["amount"] for o in outputs]
    )

    if raw_tx_data is not None:
        tx.raw_tx_data = raw_tx_data

    return tx


async def get_nonce(server, account_address, chain_id, asset_id=1):
    balance_info = await get_balance(server, account_address, chain_id, asset_id)
    return balance_info["nonce"]
