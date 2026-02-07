import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from lib.db import fetch_all, fetch_one
from lib.hex import is_hex, normalize_hex


@st.cache_data(ttl=30)
def get_address_summary(address_hex: str) -> dict | None:
    return fetch_one(
        """
        SELECT
          encode(address, 'hex') AS address_hex,
          COUNT(*) AS tx_count,
          MAX(block_number) AS latest_block,
          MIN(block_number) AS first_block
        FROM address_tx
        WHERE address = decode(%s, 'hex')
        GROUP BY address
        """,
        (address_hex,),
    )


@st.cache_data(ttl=20)
def get_address_activity(address_hex: str, limit: int = 100) -> list[dict]:
    return fetch_all(
        """
        SELECT
          block_number,
          transaction_index,
          encode(transaction_hash, 'hex') AS transaction_hash_hex,
          direction,
          encode(counterparty, 'hex') AS counterparty_hex,
          value_string,
          success,
          block_timestamp
        FROM address_tx
        WHERE address = decode(%s, 'hex')
        ORDER BY block_number DESC, transaction_index DESC
        LIMIT %s
        """,
        (address_hex, limit),
    )


st.title("Address")
address = st.text_input("Address (0x...)")
if not address:
    st.stop()

needle = normalize_hex(address)
if len(needle) != 40:
    st.error("Address must be 20 bytes (40 hex chars).")
    st.stop()
if not is_hex(needle):
    st.error("Address contains non-hex characters.")
    st.stop()

summary = get_address_summary(needle)
if not summary:
    st.warning("Address not found.")
    st.stop()

st.subheader("Summary")
st.json(summary)

st.subheader("Recent Activity")
activity = get_address_activity(needle)
if activity:
    st.dataframe(activity, use_container_width=True)
else:
    st.info("No activity found.")
