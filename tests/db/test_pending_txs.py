from typing import Sequence

import pytest
from aleph_message.models import Chain

from aleph.db.accessors.pending_txs import get_pending_txs_stream, count_pending_txs
from aleph.db.models import ChainTxDb, PendingTxDb, ChainSyncProtocol
import datetime as dt

from aleph.types.db_session import DbSessionFactory


@pytest.fixture
def fixture_txs() -> Sequence[PendingTxDb]:
    return [
        PendingTxDb(
            protocol=ChainSyncProtocol.OffChain,
            protocol_version=1,
            content="1",
            tx=ChainTxDb(
                hash="1",
                chain=Chain.ETH,
                height=1200,
                datetime=dt.datetime(2022, 1, 1),
                publisher="0xabadbabe",
            )
        ),
        PendingTxDb(
            protocol=ChainSyncProtocol.OffChain,
            protocol_version=1,
            content="2",
            tx=ChainTxDb(
                hash="2",
                chain=Chain.SOL,
                height=30000000,
                datetime=dt.datetime(2022, 1, 2),
                publisher="SOLMATE",
            )
        ),
        PendingTxDb(
            protocol=ChainSyncProtocol.OffChain,
            protocol_version=1,
            content="3",
            tx=ChainTxDb(
                hash="3",
                chain=Chain.ETH,
                height=1202,
                datetime=dt.datetime(2022, 1, 3),
                publisher="0xabadbabe",
            )
        ),
    ]


def assert_pending_txs_equal(expected: PendingTxDb, actual: PendingTxDb):
    assert expected.protocol == actual.protocol
    assert expected.protocol_version == actual.protocol_version
    assert expected.content == actual.content
    assert expected.tx_hash == actual.tx_hash


@pytest.mark.asyncio
async def test_get_pending_txs(
    session_factory: DbSessionFactory, fixture_txs: Sequence[PendingTxDb]
):
    with session_factory() as session:
        session.add_all(fixture_txs)
        session.commit()

    with session_factory() as session:
        pending_txs = list(await get_pending_txs_stream(session=session))

    for expected_tx, actual_tx in zip(pending_txs, fixture_txs):
        assert_pending_txs_equal(expected_tx, actual_tx)

    # Test the limit parameter
    with session_factory() as session:
        pending_txs = list(await get_pending_txs_stream(session=session, limit=1))

    assert pending_txs
    assert len(pending_txs) == 1
    assert_pending_txs_equal(fixture_txs[0], pending_txs[0])


@pytest.mark.asyncio
async def test_count_pending_txs(
    session_factory: DbSessionFactory, fixture_txs: Sequence[PendingTxDb]
):
    with session_factory() as session:
        session.add_all(fixture_txs)
        session.commit()

    with session_factory() as session:
        nb_txs = await count_pending_txs(session=session)

    assert nb_txs == len(fixture_txs)
