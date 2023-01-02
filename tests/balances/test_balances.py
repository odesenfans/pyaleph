from typing import Optional, Mapping, Any

import pytest
from aleph_message.models import Chain

from aleph.db.accessors.balances import get_balance_by_chain
from aleph.db.models import AlephBalanceDb
from aleph.handlers.content.post import update_balances
from aleph.types.db_session import DbSessionFactory, DbSession

BALANCES_CONTENT_SOL: Mapping[str, Any] = {
    "tags": ["SOL", "SPL", "CsZ5LZkDS7h9TDKjrbL7VAwQZ9nsRu8vJLhRYfmGaN8K", "mainnet"],
    "chain": "SOL",
    "balances": {
        "18qhTFQujXfKpQERMsagphko8mnuKycvZGZcGfKX1V9": 299.152878,
        "1DvJzfHTPmTj4EVf4Rf4iHWPTRRR4jUpgm5HaXJhYBd": 3103.90945,
        "1Q7bSc4ZKqGeGeRhHSN6ATeVkRm6oWDbrLvmMFLjNTc": 0.055448,
        "1nc1nerator11111111111111111111111111111111": 0.018864,
        "1seeWthuL3XEGT9VY6bThgeSA9mfSpWyy9xAYtQYGwP": 100000.166466,
    },
    "platform": "ALEPH_SOL",
    "main_height": 14270470,
    "token_symbol": "ALEPH",
    "token_contract": "CsZ5LZkDS7h9TDKjrbL7VAwQZ9nsRu8vJLhRYfmGaN8K",
}

BALANCES_CONTENT_SOL_UPDATE: Mapping[str, Any] = {
    "tags": ["SOL", "SPL", "CsZ5LZkDS7h9TDKjrbL7VAwQZ9nsRu8vJLhRYfmGaN8K", "mainnet"],
    "chain": "SOL",
    "balances": {
        "18qhTFQujXfKpQERMsagphko8mnuKycvZGZcGfKX1V9": 4.0,
        "1DvJzfHTPmTj4EVf4Rf4iHWPTRRR4jUpgm5HaXJhYBd": 3.0,
        "1Q7bSc4ZKqGeGeRhHSN6ATeVkRm6oWDbrLvmMFLjNTc": 2.0,
        "1nc1nerator11111111111111111111111111111111": 1.0,
        "1seeWthuL3XEGT9VY6bThgeSA9mfSpWyy9xAYtQYGwP": 0.0,
        "aU64L8cyqcZpPhGsSsgtQGinH2sF4sTmFpoMmL9t2f": 0.129155,
    },
    "platform": "ALEPH_SOL",
    "main_height": 14270471,
    "token_symbol": "ALEPH",
    "token_contract": "CsZ5LZkDS7h9TDKjrbL7VAwQZ9nsRu8vJLhRYfmGaN8K",
}

BALANCES_CONTENT_SABLIER: Mapping[str, Any] = {
    "dapp": "SABLIER",
    "tags": ["SABLIER", "0xCD18eAa163733Da39c232722cBC4E8940b1D8888", "mainnet"],
    "chain": "ETH",
    "height": 16171309,
    "dapp_id": "0xCD18eAa163733Da39c232722cBC4E8940b1D8888",
    "balances": {
        "0xC88805D05E070E12F5d82eC7773b4d64A30a219B": 12447.999999999984,
        "0xa58Cc23a546b6cE08EE258cfb54D92d4cC151Ba4": 4.9999999999999964,
        "0xc6455E6A363b1713C3fe19C94a99731F9Cb63a57": 32180.01277139208,
        "0xdaC688FDca619b43248962272b9C3BA5427B1E00": 153542.07643202206,
        "0xe4D157744E07Db9d74CeB66EFbD5C7C7e0F20b96": 1125000.0,
    },
    "platform": "ALEPH_ETH_SABLIER",
    "network_id": 1,
    "main_height": 16171309,
    "token_symbol": "ALEPH",
    "token_contract": "0x27702a26126e0B3702af63Ee09aC4d1A084EF628",
}


def compare_balances(
    session: DbSession, balances: Mapping[str, float], chain: Chain, dapp: Optional[str]
):
    for address, expected_balance in balances.items():
        balance_db = get_balance_by_chain(
            session, address=address, chain=chain, dapp=dapp
        )
        assert balance_db is not None
        # Easier to compare decimals and floats as strings
        assert str(balance_db) == str(expected_balance)

    nb_balances_db = AlephBalanceDb.count(session)
    assert nb_balances_db == len(balances)


@pytest.mark.asyncio
async def test_process_balances_solana(session_factory: DbSessionFactory):
    content = BALANCES_CONTENT_SOL

    with session_factory() as session:
        update_balances(session=session, content=content)
        session.commit()

        balances = content["balances"]
        compare_balances(
            session=session, balances=balances, chain=Chain.SOL, dapp=None
        )


@pytest.mark.asyncio
async def test_process_balances_sablier(session_factory: DbSessionFactory):
    content = BALANCES_CONTENT_SABLIER

    with session_factory() as session:
        update_balances(session=session, content=content)
        session.commit()

        balances = content["balances"]
        compare_balances(
            session=session, balances=balances, chain=Chain.ETH, dapp="SABLIER"
        )


@pytest.mark.asyncio
async def test_update_balances(session_factory: DbSessionFactory):
    content = BALANCES_CONTENT_SOL

    with session_factory() as session:
        update_balances(session=session, content=content)
        session.commit()

    new_content = BALANCES_CONTENT_SOL_UPDATE
    with session_factory() as session:
        update_balances(session=session, content=new_content)
        session.commit()
        session.expire_all()

        compare_balances(
            session=session,
            balances=new_content["balances"],
            chain=Chain.SOL,
            dapp=None,
        )


# @pytest.mark.asyncio
# async def test_update_large_files(session_factory: DbSessionFactory):
#     fixtures_dir = Path(__file__).parent / "fixtures"
#     file_1 = fixtures_dir / "large_balance_file_1.json"
#     file_2 = fixtures_dir / "large_balance_file_2.json"
#
#     with file_1.open() as f:
#         content_1 = json.load(f)
#
#     with file_2.open() as f:
#         content_2 = json.load(f)
#
#     with session_factory() as session:
#         await update_balances(session, content_1)
#         session.commit()
#
#     with session_factory() as session:
#         await update_balances(session, content_2)
#         session.commit()
