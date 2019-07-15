""" Storage module for Aleph.
Basically manages the IPFS storage.
"""

import aioipfs
import aiohttp
import asyncio
import json
import aiohttp
import concurrent

API = None

async def get_base_url(config):
    return 'http://{}:{}'.format(config.ipfs.host.value,
                                 config.ipfs.port.value)


async def get_ipfs_gateway_url(config, hash):
    return 'http://{}:{}/ipfs/{}'.format(
        config.ipfs.host.value,
        config.ipfs.gateway_port.value, hash)


async def get_ipfs_api(timeout=60):
    global API
    if API is None:
        from aleph.web import app
        host = app['config'].ipfs.host.value
        port = app['config'].ipfs.port.value

        API = aioipfs.AsyncIPFS(host=host, port=port,
                                read_timeout=timeout,
                                conns_max=100)

    return API


async def get_json(hash, timeout=5, tries=3):
    from aleph.web import app
    async with aiohttp.ClientSession(read_timeout=timeout) as session:
        uri = await get_ipfs_gateway_url(app['config'], hash)
        try_count = 0
        result = None
        while (result is None) and (try_count < tries):
            try_count += 1
            try:
                async with session.get(uri) as resp:
                    result = await resp.json(content_type=None)
                # result = await api.cat(hash)
                # result = json.loads(result)
            except (concurrent.futures.TimeoutError):
                result = None
                await asyncio.sleep(.5)
            except json.JSONDecodeError:
                result = None
                break
            except (concurrent.futures.CancelledError,
                    aiohttp.client_exceptions.ClientConnectorError):
                try_count -= 1  # do not count as a try.
                await asyncio.sleep(.1)

        return result


async def add_json(value):
    # loop = asyncio.get_event_loop()
    api = await get_ipfs_api(timeout=5)
    # try:
    result = await api.add_json(value)
    # finally:
    #     await api.close()

    return result['Hash']


async def pin_add(hash, timeout=5, tries=3):
    # loop = asyncio.get_event_loop()
    try_count = 0
    result = None
    while (result is None) and (try_count < tries):
        try_count += 1
        api = await get_ipfs_api(timeout=timeout)
        try:
            result = None
            async for ret in api.pin.add(hash):
                result = ret
        except (concurrent.futures.TimeoutError, json.JSONDecodeError):
            result = None
        except concurrent.futures.CancelledError:
            try_count -= 1  # do not count as a try.
            await asyncio.sleep(.1)
        # finally:
        #     await api.close()

    return result


async def add_file(fileobject, filename):
    async with aiohttp.ClientSession() as session:
        from aleph.web import app
        url = "%s/api/v0/add" % (await get_base_url(app['config']))
        data = aiohttp.FormData()
        data.add_field('path',
                       fileobject,
                       filename=filename)

        resp = await session.post(url, data=data)
        return await resp.json()
