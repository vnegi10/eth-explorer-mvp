import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from lib.db import fetch_all, fetch_one
from lib.hex import is_hex, normalize_hex
from lib.time_utils import format_row_timestamps
from lib.value_utils import format_value_string_as_eth


@st.cache_data(ttl=30)
def get_address_summary(address_hex: str) -> dict | None:
    return fetch_one(
        """
        WITH summary AS (
          SELECT
            encode(address, 'hex') AS address_hex,
            COUNT(*) AS tx_count,
            MAX(block_number) AS latest_block,
            MIN(block_number) AS first_block
          FROM address_tx
          WHERE address = decode(%s, 'hex')
          GROUP BY address
        )
        SELECT
          s.address_hex,
          s.tx_count,
          s.latest_block,
          lb.timestamp AS latest_block_timestamp,
          s.first_block,
          fb.timestamp AS first_block_timestamp
        FROM summary s
        LEFT JOIN blocks lb ON lb.block_number = s.latest_block
        LEFT JOIN blocks fb ON fb.block_number = s.first_block
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
address = st.text_input("Address (0x...)", key="address_query")
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
st.json(format_row_timestamps(summary, keys=("latest_block_timestamp", "first_block_timestamp")))

st.subheader("Recent Activity")
activity = get_address_activity(needle)
if activity:
    st.dataframe(
        [format_value_string_as_eth(format_row_timestamps(row)) for row in activity],
        width="stretch",
    )
else:
    st.info("No activity found.")
