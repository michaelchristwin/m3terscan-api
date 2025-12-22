"""
Valkey client.
"""

import os
from typing import Optional
from dotenv import load_dotenv
from glide import GlideClientConfiguration, NodeAddress, GlideClient

load_dotenv(dotenv_path=".env.local")


class ValkeyManager:
    """Manages the lifecycle of the GlideClient connection."""

    _client: Optional[GlideClient] = None
    _host: str = os.getenv("VALKEY_HOST", "")

    @classmethod
    async def init(cls) -> None:
        """Initializes the client if it doesn't already exist."""
        if not cls._host:
            raise KeyError("VALKEY_HOST is not set.")

        if cls._client is None:
            addresses = [NodeAddress(cls._host, 6379)]
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
