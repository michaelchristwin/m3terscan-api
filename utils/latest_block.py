"""
Utility file for fetching the block anchor.
"""

import datetime
import requests

ARWEAVE = "https://arweave.net"


def get_latest_block():
    """
    Function to get the latest block height and it's timestamp from arweave.
    """
    resp = requests.get(f"{ARWEAVE}/info", timeout=5000)
    info = resp.json()
    height = info["height"]
    block_resp = requests.get(f"{ARWEAVE}/block/height/{height}", timeout=5000)
    height_data = block_resp.json()
    height_timestamp = height_data["timestamp"]
    dt = datetime.datetime.fromtimestamp(height_timestamp)
    return height, dt


def get_block_by_height(h: int):
    """
    Get block by height.

     :param h: Description
     :type h: int
    """
    r = requests.get(
        f"{ARWEAVE}/block/height/{h}",
        timeout=5000,
        headers={"Accept-Encoding": "identity"},
    )
    r.raise_for_status()
    return r.json()


def get_network_info():
    """
    Get network info.
    """
    r = requests.get(
        f"{ARWEAVE}/info", timeout=5000, headers={"Accept-Encoding": "identity"}
    )
    r.raise_for_status()
    return r.json()


def first_block_of_day(year: int, month: int, day: int):
    """
    Helper function to get first block of a specified day.

     :param year: Description
     :param month: Description
     :param day: Description
    """
    # convert 00:00:00 UTC to unix timestamp
    t0 = int(
        datetime.datetime(year, month, day, tzinfo=datetime.timezone.utc).timestamp()
    )

    info = get_network_info()
    high = info["height"]
    low = 0

    # binary search
    while low + 1 < high:
        mid = (low + high) // 2
        block = get_block_by_height(mid)
        ts = block["timestamp"]

        if ts >= t0:
            high = mid
        else:
            low = mid

    # high should now be the first block >= t0
    return high
