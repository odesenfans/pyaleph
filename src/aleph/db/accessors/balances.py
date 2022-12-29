from decimal import Decimal
from typing import Optional

from aleph_message.models import Chain
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from aleph.db.models import AlephBalanceDb
from aleph.types.db_session import DbSession


async def get_balance_by_chain(
    session: DbSession, address: str, chain: Chain, dapp: Optional[str] = None
) -> Optional[Decimal]:
    return session.execute(
        select(AlephBalanceDb.balance).where(
            (AlephBalanceDb.address == address)
            & (AlephBalanceDb.chain == chain)
            & (AlephBalanceDb.dapp == dapp)
        )
    ).scalar()


async def update_balance(
    session: DbSession,
    address: str,
    chain: Chain,
    dapp: Optional[str],
    eth_height: int,
    balance: Decimal,
):
    insert_stmt = insert(AlephBalanceDb).values(
        address=address, chain=chain, dapp=dapp, eth_height=eth_height, balance=balance
    )
    upsert_stmt = insert_stmt.on_conflict_do_update(
        constraint="balances_pkey",
        set_={"eth_height": eth_height, "balance": balance},
        where=AlephBalanceDb.eth_height < eth_height,
    )
    session.execute(upsert_stmt)
