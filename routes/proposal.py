"""
APIRouter for the proposal endpoint.
"""

import sqlite3
from typing import List
from fastapi import APIRouter
from fastapi import HTTPException
from eth_typing import HexStr
from web3 import Web3
from orm import load_db
from config import eth_rpc
from models import output
from contracts import loader
from utils import helpers

rollup_abi = loader.load_abi("Rollup")

proposal_router = APIRouter(prefix="/proposal")


@proposal_router.get("/{tx_hash}", response_model=List[output.ProposalsResponse])
def get_proposal(tx_hash: str):
    """
    Retrieve proposal details by transaction hash.

    First checks the database cache. If not found, fetches from Ethereum
    blockchain, decodes the transaction data, and caches the results.

    :param tx_hash: Ethereum transaction hash
    :type tx_hash: str
    :return: List of proposal meter readings
    """
    db = load_db.get_db()

    # Try to fetch from database cache
    cached_proposals = _fetch_cached_proposals(db, tx_hash)
    if cached_proposals:
        return cached_proposals

    # Not in cache - fetch from blockchain and persist
    return _fetch_and_cache_from_blockchain(db, tx_hash)


def _fetch_cached_proposals(db, tx_hash: str) -> List[output.ProposalsResponse]:
    """Fetch proposals from database cache."""
    try:
        rows = db.execute(
            "SELECT m3ter_no, account, nonce FROM proposal_meters WHERE proposal_id = ?",
            (tx_hash,),
        ).fetchall()
    except Exception as err:
        raise HTTPException(
            status_code=500, detail=f"Database error: {str(err)}"
        ) from err

    if not rows:
        return []

    return [
        output.ProposalsResponse(
            m3ter_no=int(row["m3ter_no"]),
            account=row["account"],
            nonce=int(row["nonce"]),
        )
        for row in rows
    ]


def _fetch_and_cache_from_blockchain(
    db, tx_hash: str
) -> List[output.ProposalsResponse]:
    """Fetch proposal from Ethereum blockchain and cache in database."""
    # Fetch and decode transaction
    eth_client = eth_rpc.w3
    tx = eth_client.eth.get_transaction(transaction_hash=HexStr(tx_hash))
    input_data = tx.get("input")

    # Decode contract function call
    rollup_address = "0xf8f2d4315DB5db38f3e5c45D0bCd59959c603d9b"
    rollup = eth_client.eth.contract(
        address=Web3.to_checksum_address(rollup_address),
        abi=rollup_abi,
    )
    _, args = rollup.decode_function_input(input_data)

    # Parse meter data from blobs
    account_chunks = helpers.bytes_to_chunks(args["accountBlob"], 6)
    nonce_chunks = helpers.bytes_to_chunks(args["nonceBlob"], 6)
    meters = helpers.combine_accounts_nonces(
        accounts=account_chunks, nonces=nonce_chunks
    )

    # Persist to database
    _cache_proposals(db, tx_hash, meters)

    return meters


def _cache_proposals(db, tx_hash: str, meters: List[output.ProposalsResponse]) -> None:
    """Store proposal and meters in database."""
    db.row_factory = sqlite3.Row
    cursor = db.cursor()

    try:
        db.execute("BEGIN")

        # Create proposal record
        cursor.execute("INSERT INTO proposals (id) VALUES (?)", (tx_hash,))

        # Insert all meter readings
        cursor.executemany(
            """
            INSERT INTO proposal_meters (proposal_id, m3ter_no, account, nonce)
            VALUES (?, ?, ?, ?)
            """,
            [
                (tx_hash, meter.m3ter_no, meter.account, str(meter.nonce))
                for meter in meters
            ],
        )

        db.commit()
    except Exception as err:
        db.rollback()
        raise HTTPException(
            status_code=500, detail=f"Failed to cache proposals: {str(err)}"
        ) from err
