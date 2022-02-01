"""Tests to validate the sending and receiving of messages using the Aleph P2P protocol."""


import asyncio
from typing import Tuple

import pytest
from p2pclient import Client as P2PClient

from aleph.services.p2p.protocol import AlephProtocol


@pytest.mark.asyncio
@pytest.mark.parametrize("p2p_clients", [2], indirect=True)
async def test_pubsub(p2p_clients: Tuple[P2PClient, P2PClient]):
    client1, client2 = p2p_clients
    protocol1 = AlephProtocol.create(client1)
    protocol2 = AlephProtocol.create(client2)
