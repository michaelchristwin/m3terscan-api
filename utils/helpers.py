"""
Helper functions for decoding blockchain data.
"""

from typing import List
from models import output


def bytes_to_chunks(data: bytes, chunk_size: int) -> List[int]:
    """
    Converts a byte array into a list of large integers by splitting the data
    into fixed-size chunks.

    The implementation prepends a single 0x00 byte to the data before chunking,
    matching the behavior of the original Go function.

    :param data: The raw input bytes to be chunked.
    :param chunk_size: The desired size of each byte chunk.
    :raises ValueError: If the chunk_size is zero or negative.
    :return: A list of integers, where each integer represents a chunk of the bytes.
    """
    if chunk_size <= 0:
        raise ValueError("chunk_size must be a positive integer.")
    padded_data = b"\x00" + data

    result: List[int] = []
    data_length = len(padded_data)

    # 2. Iterate through the padded data, slicing it into chunks
    for i in range(0, data_length, chunk_size):
        # Determine the end index for the current chunk
        end_index = i + chunk_size
        end_index = min(end_index, data_length)
        part = padded_data[i:end_index]
        num = int.from_bytes(part, byteorder="big")

        result.append(num)

    return result


# --- 2. The Core Helper Function ---


def combine_accounts_nonces(
    accounts: List[int], nonces: List[int]
) -> List[output.ProposalsResponse]:
    """
    Combines two lists of large integers (accounts and nonces) into a
    list of structured ProposalsResponse objects.

    The 'account' integer is treated as a fixed-point number with 6 decimal
    places, split, and formatted into a string like "123.456789".

    :param accounts: A list of large integers representing encoded account IDs.
    :param nonces: A list of large integers representing nonces.
    :raises ValueError: If the lists of accounts and nonces have different lengths.
    :return: A list of ProposalsResponse objects.
    """
    if len(accounts) != len(nonces):
        raise ValueError("accounts and nonces must have the same length.")

    # Corresponds to 1_000_000 (10^6) in the Go code, used for fixed-point math.
    decimals = 1_000_000

    result: List[output.ProposalsResponse] = []

    for i, account_int in enumerate(accounts):
        int_part = account_int // decimals
        frac_part = account_int % decimals
        formatted_account = f"{int_part}.{frac_part:06d}"

        result.append(
            output.ProposalsResponse(
                m3ter_no=i, account=formatted_account, nonce=nonces[i]
            )
        )

    return result
