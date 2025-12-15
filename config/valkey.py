"""
Valkey client.
"""

from glide import GlideClientConfiguration, NodeAddress, GlideClient


async def get_client():
    """
    Docstring for get_client
    """
    addresses = [NodeAddress("localhost", 6379)]
    config = GlideClientConfiguration(addresses, request_timeout=500)
    client = await GlideClient.create(config)
    return client
