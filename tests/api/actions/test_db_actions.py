import datetime as dt

import pytest
from aleph_message.models import Chain
from sqlalchemy import insert, select

from aleph.db.models import ChainTxDb
from aleph.types.actions.action import ActionStatus
from aleph.types.actions.db_action import DbAction
from aleph.types.actions.db_executor import DbExecutor
from aleph.types.actions.executor import execute_actions


class TestDbAction(DbAction):
    def __init__(self, chain_tx: ChainTxDb):
        super().__init__()
        self.chain_tx = chain_tx

    def to_db_statements(self):
        return [insert(ChainTxDb).values(**self.chain_tx.to_dict())]


@pytest.mark.asyncio
async def test_execute_one_db_action(session_factory):
    chain_tx = ChainTxDb(
        hash="0x1234",
        chain=Chain.ETH,
        height=8000,
        datetime=dt.datetime(2020, 1, 1),
        publisher="0xabadbabe",
    )
    action = TestDbAction(chain_tx)
    executor = DbExecutor(session_factory=session_factory)

    await execute_actions(actions=[action], executors={TestDbAction: executor})

    assert action.status == ActionStatus.DONE
    with session_factory() as session:
        chain_tx_db = session.execute(select(ChainTxDb)).scalar_one()

    assert chain_tx_db.hash == chain_tx.hash
    assert chain_tx_db.height == chain_tx.height
    assert chain_tx_db.publisher == chain_tx.publisher


@pytest.mark.asyncio
async def test_execute_multiple_db_actions_success(session_factory):
    chain_tx_1 = ChainTxDb(
        hash="0x1234",
        chain=Chain.ETH,
        height=8000,
        datetime=dt.datetime(2020, 1, 1),
        publisher="0xabadbabe",
    )
    chain_tx_2 = ChainTxDb(
        hash="0x1235",
        chain=Chain.SOL,
        height=1000,
        datetime=dt.datetime(1987, 10, 10),
        publisher="solana",
    )
    chain_tx_3 = ChainTxDb(
        hash="0xdeadbeef",
        chain=Chain.TEZOS,
        height=10000,
        datetime=dt.datetime(2022, 2, 22),
        publisher="b0rg",
    )

    executor = DbExecutor(session_factory=session_factory)
    actions = [
        TestDbAction(chain_tx) for chain_tx in (chain_tx_1, chain_tx_2, chain_tx_3)
    ]

    await execute_actions(actions=actions, executors={TestDbAction: executor})

    for action in actions:
        assert action.status == ActionStatus.DONE

    with session_factory() as session:
        chain_txs_db = list(session.execute(select(ChainTxDb)).scalars())
    assert len(chain_txs_db) == 3


@pytest.mark.asyncio
async def test_execute_multiple_db_actions_primary_key_conflict(session_factory):
    chain_tx = ChainTxDb(
        hash="0x1234",
        chain=Chain.ETH,
        height=8000,
        datetime=dt.datetime(2020, 1, 1),
        publisher="0xabadbabe",
    )

    executor = DbExecutor(session_factory=session_factory)
    actions = [TestDbAction(chain_tx), TestDbAction(chain_tx)]

    await execute_actions(actions=actions, executors={TestDbAction: executor})

    assert actions[0].status == ActionStatus.DONE
    assert actions[1].status == ActionStatus.FAILED

    with session_factory() as session:
        chain_tx_db = session.execute(select(ChainTxDb)).scalar_one()

    assert chain_tx_db.hash == chain_tx.hash
