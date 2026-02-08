import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from lib.db import fetch_one
from lib.hex import is_hex, normalize_hex
from lib.time_utils import format_row_timestamps
from lib.value_utils import format_value_string_as_eth, wei_int_to_eth_display, wei_int_to_gwei_display


@st.cache_data(ttl=30)
def get_tx(tx_hash_hex: str) -> dict | None:
    return fetch_one(
        """
        SELECT
          encode(transaction_hash, 'hex') AS transaction_hash_hex,
          block_number,
          transaction_index,
          nonce,
          encode(from_address, 'hex') AS from_address_hex,
          encode(to_address, 'hex') AS to_address_hex,
          value_string,
          gas_limit,
          gas_used,
          gas_price,
          transaction_type,
          success,
          block_timestamp
        FROM tx
        WHERE transaction_hash = decode(%s, 'hex')
        """,
        (tx_hash_hex,),
    )


st.title("Transaction")
tx_hash = st.text_input("Transaction hash (0x...)", key="tx_hash_query")
if not tx_hash:
    st.stop()

needle = normalize_hex(tx_hash)
if len(needle) != 64:
    st.error("Transaction hash must be 32 bytes (64 hex chars).")
    st.stop()
if not is_hex(needle):
    st.error("Transaction hash contains non-hex characters.")
    st.stop()

tx = get_tx(needle)
if not tx:
    st.warning("Transaction not found.")
    st.stop()

tx_out = format_value_string_as_eth(format_row_timestamps(tx))
gas_price = tx_out.get("gas_price")
gas_used = tx_out.get("gas_used")
tx_out["gas_price"] = wei_int_to_gwei_display(gas_price)
if isinstance(gas_price, int) and isinstance(gas_used, int):
    tx_out["transaction_fee_eth"] = wei_int_to_eth_display(gas_price * gas_used)
else:
    tx_out["transaction_fee_eth"] = None

st.json(tx_out)
