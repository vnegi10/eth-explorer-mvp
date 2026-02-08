import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from lib.db import fetch_all, fetch_one
from lib.time_utils import format_row_timestamps
from lib.value_utils import format_value_string_as_eth


@st.cache_data(ttl=30)
def get_block(block_number: int) -> dict | None:
    return fetch_one(
        """
        SELECT
          block_number,
          timestamp,
          gas_used,
          base_fee_per_gas,
          chain_id,
          encode(block_hash, 'hex') AS block_hash_hex,
          encode(author, 'hex') AS author_hex
        FROM blocks
        WHERE block_number = %s
        """,
        (block_number,),
    )


@st.cache_data(ttl=20)
def get_block_txs(block_number: int, limit: int = 100) -> list[dict]:
    return fetch_all(
        """
        SELECT
          transaction_index,
          encode(transaction_hash, 'hex') AS transaction_hash_hex,
          encode(from_address, 'hex') AS from_address_hex,
          encode(to_address, 'hex') AS to_address_hex,
          value_string,
          gas_used,
          success
        FROM tx
        WHERE block_number = %s
        ORDER BY transaction_index ASC
        LIMIT %s
        """,
        (block_number, limit),
    )


st.title("Block")
block_number = st.number_input("Block number", min_value=0, step=1, value=0)

block = get_block(int(block_number))
if not block:
    st.warning("Block not found.")
    st.stop()

st.subheader("Block Details")
st.json(format_row_timestamps(block))

st.subheader("Transactions")
txs = get_block_txs(int(block_number))
if txs:
    st.dataframe([format_value_string_as_eth(row) for row in txs], use_container_width=True)
else:
    st.info("No transactions for this block.")
