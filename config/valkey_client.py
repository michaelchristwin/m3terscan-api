"""
Valkey client.
"""

import os
from typing import Optional
from urllib.parse import urlparse

from dotenv import load_dotenv
from glide import GlideClient, GlideClientConfiguration, NodeAddress, ServerCredentials

load_dotenv(dotenv_path=".env.local")


class ValkeyManager:
    """Manages the lifecycle of the GlideClient connection."""

    _client: Optional[GlideClient] = None
    _url = os.getenv("VALKEY_URL")

    @classmethod
    async def init(cls):
        if not cls._url:
            raise KeyError("VALKEY_URL not set")

        parsed = urlparse(cls._url)
        # Extract authentication (if present)
        username = parsed.username
        password = parsed.password

        host = parsed.hostname or ""
        port = parsed.port or 6379

        addresses = [NodeAddress(host, port)]
        if username or password:
            config = GlideClientConfiguration(
                addresses,
                request_timeout=500,
                credentials=ServerCredentials(password=password, username=username),
            )
        else:
            config = GlideClientConfiguration(addresses, request_timeout=500)

        cls._client = await GlideClient.create(config)

    @classmethod
    async def close(cls) -> None:
        """Closes the client connection and resets the state."""
        if cls._client:
            await cls._client.close()
            cls._client = None

    @classmethod
    def get_client(cls) -> GlideClient:
        """Returns the active client instance."""
        if cls._client is None:
            raise RuntimeError("Valkey client not initialized. Call init() first.")
        return cls._client
