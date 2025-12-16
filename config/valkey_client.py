"""
Valkey client.
"""

import os
from dotenv import load_dotenv
from glide import GlideClientConfiguration, NodeAddress, GlideClient

load_dotenv(dotenv_path=".env.local")

VALKEY_HOST = os.getenv("VALKEY_HOST")


async def get_client():
    """
    Docstring for get_client
    """
    if VALKEY_HOST is None:
        raise KeyError("VALKEY_HOST is not set.")
    addresses = [NodeAddress(VALKEY_HOST, 6379)]
    config = GlideClientConfiguration(addresses, request_timeout=500)
    client = await GlideClient.create(config)
    return client
