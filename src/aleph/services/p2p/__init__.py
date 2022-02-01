import socket
from typing import Coroutine, List, Tuple

from configmanager import Config
from multiaddr import Multiaddr
from p2pclient import Client as P2PClient

from . import singleton
from .manager import initialize_host


def init_p2p_client(config: Config) -> P2PClient:
    host = config.p2p.host.value
    host_ip_addr = socket.gethostbyname(host)

    control_port = config.p2p.control_port.value
    listen_port = config.p2p.listen_port.value
    control_maddr = Multiaddr(f"/ip4/{host_ip_addr}/tcp/{control_port}")
    listen_maddr = Multiaddr(f"/ip4/0.0.0.0/tcp/{listen_port}")
    p2p_client = P2PClient(control_maddr=control_maddr, listen_maddr=listen_maddr)

    return p2p_client


async def init_p2p(config: Config, listen: bool = True, port_id: int = 0) -> Tuple[P2PClient, List[Coroutine]]:
    p2p_client = init_p2p_client(config)
    # This singleton will not be required anymore once the API is in its own separate program.
    singleton.client = p2p_client

    port = config.p2p.port.value + port_id
    singleton.streamer, tasks = await initialize_host(
        config=config,
        p2p_client=p2p_client,
        host=config.p2p.host.value,
        port=port,
        listen=listen,
        protocol_active=("protocol" in config.p2p.clients.value),
    )

    return p2p_client, tasks
