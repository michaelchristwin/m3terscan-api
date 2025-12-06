"""
Docstring for config.eth-rpc
"""

import os
from dotenv import load_dotenv
from web3 import Web3

load_dotenv(dotenv_path=".env.local")

API_KEY = os.getenv("INFURA_API_KEY")
w3 = Web3(Web3.HTTPProvider(f"https://sepolia.infura.io/v3/{API_KEY}"))
