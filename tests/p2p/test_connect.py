"""Tests to validate the connection / reconnection features of the P2P service."""

from typing import Tuple

import pytest
from p2pclient import Client as P2PClient


@pytest.mark.asyncio
@pytest.mark.parametrize("p2p_clients", [2], indirect=True)
async def test_p2p_client_connect(p2p_clients: Tuple[P2PClient, P2PClient]):
    """
    Sanity check: verify that connecting two peers makes each peer appear in the peer list of the other peer.
    This test is redundant with some tests in p2pclient itself.
    """
    client1, client2 = p2p_clients
    client1_peer_id, client1_maddrs = await client1.identify()
    client2_peer_id, client2_maddrs = await client2.identify()
    await client2.connect(client1_peer_id, client1_maddrs)

    client1_peers = await client1.list_peers()
    client2_peers = await client2.list_peers()
    assert client1_peer_id in [peer.peer_id for peer in client2_peers]
    assert client2_peer_id in [peer.peer_id for peer in client1_peers]
