import ipaddress
import logging
import re
import socket
from typing import Literal

import aiohttp

logger = logging.getLogger(__name__)

IP4_SERVICE_URL = "https://v4.ident.me/"
IP6_SERVICE_URL = "https://v6.ident.me/"
IP4_SOCKET_ENDPOINT = "8.8.8.8"
IP6_SOCKET_ENDPOINT = "2001:4860:4860::8888"


def is_valid_ip(address: str) -> bool:
    try:
        _ = ipaddress.ip_address(address)
        return True
    except ValueError:
        return False


async def get_ip_from_service(ip_version: Literal[4, 6]) -> str:
    ip_service_url = IP4_SERVICE_URL if ip_version == 4 else IP6_SERVICE_URL
    async with aiohttp.ClientSession() as session:
        async with session.get(ip_service_url) as resp:
            resp.raise_for_status()
            ip = await resp.text()

            if is_valid_ip(ip):
                return ip
            else:
                raise ValueError(f"Response is not a valid IP address format: {ip}")


async def get_ip4_from_service() -> str:
    """Get the public IPv4 of this system by calling a third-party service"""
    async with aiohttp.ClientSession() as session:
        async with session.get(IP4_SERVICE_URL) as resp:
            resp.raise_for_status()
            ip = await resp.text()

            if is_valid_ip(ip):
                return ip
            else:
                raise ValueError(f"Response does not match IPv4 format: {ip}")


def get_ip_from_socket(ip_version: Literal[4, 6]) -> str:
    """Get the public IPv4 of this system by inspecting a socket connection.
    Warning: This returns a local IP address when running behind a NAT, e.g. on Docker.
    """
    socket_family = socket.AF_INET if ip_version == 4 else socket.AF_INET6
    socket_endpoint = IP4_SOCKET_ENDPOINT if ip_version == 4 else IP6_SOCKET_ENDPOINT
    s = socket.socket(socket_family, socket.SOCK_DGRAM)
    try:
        s.connect((socket_endpoint, 80))
        return s.getsockname()[0]
    finally:
        s.close()


async def get_public_ip(ip_version: Literal[4, 6]) -> str:
    """Get the public IP address (v4 or v6) of this system."""
    try:
        return await get_ip_from_service(ip_version=ip_version)
    except Exception:
        logging.exception("Error fetching IP address from service")
        return get_ip_from_socket(ip_version=ip_version)
