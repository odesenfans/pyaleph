import asyncio
import aiohttp
import json
import time
import pkg_resources
from operator import itemgetter
from aleph.network import check_message
from aleph.chains.common import incoming, get_verification_buffer
from aleph.chains.register import (
    register_verifier, register_incoming_worker, register_outgoing_worker)
from aleph.model.chains import Chain
from aleph.model.messages import Message

from web3 import Web3
from web3.middleware import geth_poa_middleware
from web3.utils.events import get_event_data
from web3.contract import get_event_data
from web3.gas_strategies.rpc import rpc_gas_price_strategy
from eth_account.messages import defunct_hash_message
from eth_account import Account
from hexbytes import HexBytes

import logging
LOGGER = logging.getLogger('chains.ethereum')
CHAIN_NAME = 'ETH'


async def verify_signature(message):
    """ Verifies a signature of a message, return True if verified, false if not
    """
    from aleph.web import app
    config = app.config
    w3 = await get_web3(config)

    verification = await get_verification_buffer(message)

    message_hash = defunct_hash_message(text=verification)

    verified = False
    try:
        # we assume the signature is a valid string
        address = w3.eth.account.recoverHash(message_hash,
                                             signature=message['signature'])
        if address == message['sender']:
            verified = True
        else:
            LOGGER.warning('Received bad signature from %s for %s'
                           % (address, message['sender']))
            return False

    except Exception as e:
        LOGGER.exception('Error processing signature from %s for %s'
                         % (address, message['sender']))
        verified = False

    return verified

register_verifier(CHAIN_NAME, verify_signature)


async def get_last_height():
    """ Returns the last height for which we already have the ethereum data.
    """
    last_height = await Chain.get_last_height(CHAIN_NAME)

    if last_height is None:
        last_height = -1

    return last_height


async def get_web3(config):
    web3 = Web3(Web3.HTTPProvider(config.ethereum.api_url.value))
    if config.ethereum.chain_id.value == 4:  # rinkeby
        web3.middleware_stack.inject(geth_poa_middleware, layer=0)
    web3.eth.setGasPriceStrategy(rpc_gas_price_strategy)

    return web3


async def get_contract_abi():
    return json.loads(pkg_resources.resource_string(
        'aleph.chains',
        'assets/ethereum_sc_abi.json').decode('utf-8'))


async def get_contract(config, web3):
    return web3.eth.contract(config.ethereum.sync_contract.value,
                             abi=await get_contract_abi())


async def request_transactions(config, web3, contract, start_height):
    """ Continuously request data from the Ethereum blockchain.
    TODO: support websocket API.
    """
    loop = asyncio.get_event_loop()
    logs = await loop.run_in_executor(None, web3.eth.getLogs,
                                      {'address': contract.address,
                                       'fromBlock': start_height+1,
                                       'toBlock': 'latest'})

    last_height = 0

    for log in logs:
        event_data = get_event_data(contract.events.SyncEvent._get_event_abi(), log)
        publisher = event_data.args.addr  # TODO: verify rights.

        last_height = event_data.blockNumber

        message = event_data.args.message
        try:
            jdata = json.loads(message)
            if jdata.get('protocol', None) != 'aleph':
                LOGGER.info('Got unknown protocol object in tx %s'
                            % event_data.transactionHash)
                continue
            if jdata.get('version', None) != 1:
                LOGGER.info(
                    'Got an unsupported version object in tx %s'
                    % event_data.transactionHash)
                continue  # unsupported protocol version

            yield dict(type="aleph",
                       tx_hash=event_data.transactionHash,
                       height=event_data.blockNumber,
                       messages=jdata['content']['messages'])

        except json.JSONDecodeError:
            # if it's not valid json, just ignore it...
            LOGGER.info("Incoming logic data is not JSON, ignoring. %r"
                        % message)

        except Exception:
            LOGGER.exception("Can't decode incoming logic data %r"
                             % message)

    # Since we got no critical exception, save last received object
    # block height to do next requests from there.
    if last_height:
        await Chain.set_last_height(CHAIN_NAME, last_height)


async def check_incoming(config):
    last_stored_height = await get_last_height()

    LOGGER.info("Last block is #%d" % last_stored_height)
    loop = asyncio.get_event_loop()

    web3 = await get_web3(config)
    contract = await get_contract(config, web3)

    while True:
        last_stored_height = await get_last_height()
        i = 0
        j = 0

        tasks = []
        seen_ids = []
        async for txi in request_transactions(config, web3, contract,
                                              last_stored_height):
            i += 1
            # TODO: handle big message list stored in IPFS case
            # (if too much messages, an ipfs hash is stored here).
            for message in txi['messages']:
                j += 1
                message = await check_message(
                    message, from_chain=True,
                    trusted=(txi['type'] == 'native-single'))
                if message is None:
                    # message got discarded at check stage.
                    continue

                #message['time'] = txi['time']
                # TODO: handle other chain signatures here
                # now handled in check_message
                # signed = await verify_signature(message, tx=txi)

                # running those separately... a good/bad thing?
                # shouldn't do that for VMs.
                tasks.append(
                    loop.create_task(incoming(
                        message, chain_name=CHAIN_NAME,
                        seen_ids=seen_ids,
                        tx_hash=txi['tx_hash'],
                        height=txi['height'])))

                # await incoming(message, chain_name=CHAIN_NAME,
                #                tx_hash=txi['tx_hash'],
                #                height=txi['height'])

                # if txi['height'] > last_stored_height:
                #    last_stored_height = txi['height']

                # let's join every 50 messages...
                if (j > 5000):
                    for task in tasks:
                        await task
                    j = 0
                    seen_ids = []
                    tasks = []

        for task in tasks:
            await task  # let's wait for all tasks to end.

        if (i < 10):  # if there was less than 10 items, not a busy time
            # wait 5 seconds (half of typical time between 2 blocks)
            await asyncio.sleep(5)


async def ethereum_incoming_worker(config):
    while True:
        try:
            await check_incoming(config)

        except Exception:
            LOGGER.exception("ERROR, relaunching incoming in 10 seconds")
            await asyncio.sleep(10)

register_incoming_worker(CHAIN_NAME, ethereum_incoming_worker)


def broadcast_content(config, contract, web3, account,
                      gas_price, nonce, content):
    tx = contract.functions.doEmit(content).buildTransaction({
            'chainId': config.ethere.chain_id.value,
            'gasPrice': gas_price,
            'nonce': nonce,
            })
    signed_tx = account.signTransaction(tx)
    return web3.eth.sendRawTransaction(signed_tx.rawTransaction)


async def get_content_to_broadcast(messages):
    return {'protocol': 'aleph',
            'version': 1,
            'content': {
                'messages': messages
            }}


async def ethereum_packer(config):
    web3 = await get_web3(config)
    contract = await get_contract(config, web3)
    loop = asyncio.get_event_loop()

    pri_key = HexBytes(config.ethereum.private_key.value)
    account = Account.privateKeyToAccount(pri_key)
    address = account.address

    LOGGER.info("Ethereum Connector set up with address %s" % address)
    i = 0
    gas_price = web3.eth.generateGasPrice()
    while True:
        if i >= 100:
            await asyncio.sleep(30)  # wait three (!!) blocks
            gas_price = web3.eth.generateGasPrice()
            # utxo = await get_utxo(config, address)
            i = 0

        nonce = web3.eth.getTransactionCount(account.address)

        messages = [message async for message
                    in (await Message.get_unconfirmed_raw(limit=600))]

        if len(messages):
            content = await get_content_to_broadcast(messages)
            response = loop.run_in_executor(None, broadcast_content,
                                            config, contract, web3, account,
                                            gas_price, nonce, content)
            LOGGER.info("Broadcasted %r on %s" % (response, CHAIN_NAME))

        await asyncio.sleep(15)
        i += 1


async def ethereum_outgoing_worker(config):
    if config.ethereum.packing_node.value:
        while True:
            try:
                await ethereum_packer(config)

            except Exception:
                LOGGER.exception("ERROR, relaunching outgoing in 10 seconds")
                await asyncio.sleep(10)

register_outgoing_worker(CHAIN_NAME, ethereum_outgoing_worker)


async def broadcast(config, tx_hex):
    broadcast_url = '{}/broadcast'.format(
        await get_base_url(config))
    data = {'txHex': tx_hex}

    async with aiohttp.ClientSession() as session:
        async with session.post(broadcast_url, json=data) as resp:
            jres = (await resp.json())['value']
            return jres