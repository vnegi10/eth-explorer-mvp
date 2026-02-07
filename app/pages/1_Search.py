import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from lib.db import fetch_one
from lib.hex import is_hex, normalize_hex


@st.cache_data(ttl=30)
def find_block(block_number: int) -> dict | None:
    return fetch_one(
        """
        SELECT block_number, timestamp, encode(block_hash, 'hex') AS block_hash_hex
        FROM blocks
        WHERE block_number = %s
        """,
        (block_number,),
    )


@st.cache_data(ttl=30)
def find_tx(tx_hash_hex: str) -> dict | None:
    return fetch_one(
        """
        SELECT
          encode(transaction_hash, 'hex') AS transaction_hash_hex,
          block_number,
          transaction_index,
          block_timestamp
        FROM tx
        WHERE transaction_hash = decode(%s, 'hex')
        """,
        (tx_hash_hex,),
    )


@st.cache_data(ttl=30)
def find_address(address_hex: str) -> dict | None:
    return fetch_one(
        """
        SELECT
          encode(address, 'hex') AS address_hex,
          COUNT(*) AS tx_count,
          MAX(block_number) AS latest_block
        FROM address_tx
        WHERE address = decode(%s, 'hex')
        GROUP BY address
        """,
        (address_hex,),
    )


st.title("Search")
st.write("Search by block number, tx hash, or address.")

query = st.text_input("Enter block number / tx hash / address")
if not query:
    st.stop()

if query.isdigit():
    block = find_block(int(query))
    if block:
        st.success(f"Block found: {block['block_number']}")
        st.json(block)
    else:
        st.warning("Block not found.")
    st.stop()

needle = normalize_hex(query)
if len(needle) == 64:
    if not is_hex(needle):
        st.error("Invalid transaction hash: non-hex characters found.")
        st.stop()
    tx = find_tx(needle)
    if tx:
        st.success("Transaction found.")
        st.json(tx)
    else:
        st.warning("Transaction not found.")
    st.stop()

if len(needle) == 40:
    if not is_hex(needle):
        st.error("Invalid address: non-hex characters found.")
        st.stop()
    address = find_address(needle)
    if address:
        st.success("Address found.")
        st.json(address)
    else:
        st.warning("Address not found.")
    st.stop()

st.error("Unsupported input format.")
