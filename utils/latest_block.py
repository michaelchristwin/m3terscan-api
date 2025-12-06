"""
Utility file for fetching the block anchor.
"""

import datetime
import requests


def get_latest_block():
    """
    Function to get the latest block height and it's timestamp from arweave.
    """
    resp = requests.get("https://arweave.net/info", timeout=5000)
    info = resp.json()
    height = info["height"]
    block_resp = requests.get(
        f"https://arweave.net/block/height/{height}", timeout=5000
    )
    height_data = block_resp.json()
    height_timestamp = height_data["timestamp"]
    dt = datetime.datetime.fromtimestamp(height_timestamp)
    return height, dt
