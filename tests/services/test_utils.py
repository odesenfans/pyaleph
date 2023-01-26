import pytest

from aleph.services.utils import (
    is_valid_ip,
    get_ip_from_socket,
    get_public_ip,
    get_ip_from_service,
)


def test_is_valid_ip():
    assert is_valid_ip("1.2.3.4")
    assert not is_valid_ip("123.456.789.123")
    assert not is_valid_ip("")
    assert not is_valid_ip("Hello !")
    assert not is_valid_ip("a.b.c.d")


@pytest.mark.asyncio
async def test_get_ip4_from_service():
    ip4 = await get_ip_from_service(ip_version=4)
    assert is_valid_ip(ip4)

    ip6 = await get_ip_from_service(ip_version=6)
    assert is_valid_ip(ip6)


def test_get_ip_from_socket():
    ip4 = get_ip_from_socket(ip_version=4)
    assert is_valid_ip(ip4)

    ip6 = get_ip_from_socket(ip_version=6)
    assert is_valid_ip(ip6)


@pytest.mark.asyncio
async def test_get_IP():
    ip4 = await get_public_ip(ip_version=4)
    assert is_valid_ip(ip4)

    ip6 = await get_public_ip(ip_version=6)
    assert is_valid_ip(ip6)
