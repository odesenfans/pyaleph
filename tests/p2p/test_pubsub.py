import pytest
from aleph.services.p2p.pubsub import pub, sub

@pytest.mark.asyncio
@pytest.mark.parametrize("p2p_clients", [2], indirect=True)
async def test_pubsub(p2p_clients):
    topic = "test-topic"
    sub_task = sub(topic)
