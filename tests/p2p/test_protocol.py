"""Tests to validate the sending and receiving of messages using the Aleph P2P protocol."""

from dataclasses import dataclass
from typing import Tuple

import pytest
from multiaddr import Multiaddr
from p2pclient import Client as P2PClient
from p2pclient.libp2p_stubs.peer.id import ID

from aleph.services.p2p.peers import connect_peer
from aleph.services.p2p.protocol import AlephProtocol


@dataclass
class ClientData:
    p2p_client: P2PClient
    peer_id: ID
    maddrs: Tuple[Multiaddr]
    streamer: AlephProtocol

    @classmethod
    async def from_client(cls, p2p_client: P2PClient) -> "ClientData":
        peer_id, maddrs = await p2p_client.identify()
        return cls(
            p2p_client=p2p_client,
            peer_id=peer_id,
            maddrs=maddrs,
            streamer=await AlephProtocol.create(p2p_client),
        )


@pytest.fixture
async def connected_clients(p2p_clients) -> Tuple[ClientData]:
    """
    Provides P2P clients (+metadata) and ensures that they are connected together.
    """
    clients = tuple([await ClientData.from_client(p2p_client) for p2p_client in p2p_clients])
    for i, client in enumerate(clients):
        peer = clients[i-1]
        peer_maddr = f"{peer.maddrs[0]}/p2p/{peer.peer_id}"
        await connect_peer(client.p2p_client, client.streamer, peer_maddr)

    yield clients


@pytest.mark.asyncio
@pytest.mark.parametrize("p2p_clients", [2], indirect=True)
async def test_request_existing_hash(connected_clients: Tuple[ClientData, ClientData], mocker):
    expected_content = "Decentralized > Centralized"
    mocker.patch("aleph.storage.get_hash_content", return_value=expected_content)

    client1, client2 = connected_clients

    # Sanity check: ensure that the fixture did connect the peers together
    assert client1.peer_id in [peer.peer_id for peer in await client2.p2p_client.list_peers()]
    assert client2.peer_id in [peer.peer_id for peer in await client1.p2p_client.list_peers()]

    content = await client1.streamer.request_hash("123")
    assert content == expected_content
