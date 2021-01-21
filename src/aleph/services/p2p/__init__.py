from typing import Coroutine, List

from . import singleton
from .manager import initialize_host
from .peers import connect_peer
from .protocol import incoming_channel
from .pubsub import pub, sub


async def init_p2p(config, listen=True, port_id=0) -> List[Coroutine]:
    """Initialises the P2P connection.
    Returns a list of coroutines to run in the background.
    """
    pkey = config.p2p.key.value
    port = config.p2p.port.value + port_id
    singleton.host, singleton.pubsub, singleton.streamer, background_tasks =\
        await initialize_host(key=pkey, host=config.p2p.host.value,
                              port=port, listen=listen,
                              protocol_active=('protocol' in config.p2p.clients.value))
    return background_tasks
    
async def get_host():
    return singleton.host

async def get_pubsub():
    return singleton.pubsub