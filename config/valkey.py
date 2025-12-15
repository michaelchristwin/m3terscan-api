"""
Valkey client.
"""

from glide import GlideClientConfiguration, NodeAddress, GlideClient


async def get_client():
    """
    Docstring for get_client
    """
    addresses = [NodeAddress("redis://red-d4u5ac5actks73csnn9g:6379", 6379)]
    config = GlideClientConfiguration(addresses, request_timeout=500)
    client = await GlideClient.create(config)
    return client
