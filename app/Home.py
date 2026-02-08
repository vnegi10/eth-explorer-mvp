"""Main Streamlit entrypoint for the ETH explorer MVP.

All interactive data on this UI comes from Postgres serving tables.
"""

import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from lib.db import fetch_all
from lib.time_utils import format_row_timestamps
from lib.value_utils import format_value_string_as_eth


@st.cache_data(ttl=20)
def get_recent_blocks(limit: int = 20) -> list[dict]:
    return fetch_all(
        """
        SELECT
          block_number,
          timestamp,
          encode(block_hash, 'hex') AS block_hash_hex,
          gas_used
        FROM blocks
        ORDER BY block_number DESC
        LIMIT %s
        """,
        (limit,),
    )


@st.cache_data(ttl=20)
def get_recent_transactions(limit: int = 20) -> list[dict]:
    return fetch_all(
        """
        SELECT
          encode(transaction_hash, 'hex') AS transaction_hash_hex,
          block_number,
          transaction_index,
          encode(from_address, 'hex') AS from_address_hex,
          encode(to_address, 'hex') AS to_address_hex,
          value_string,
          block_timestamp
        FROM tx
        ORDER BY block_number DESC, transaction_index DESC
        LIMIT %s
        """,
        (limit,),
    )


st.set_page_config(page_title="ETH Blockchain Explorer", layout="wide")
st.title("ETH Blockchain Explorer")
st.caption(
    "Tech stack: DuckDB + Parquet for ETL/backfill, Postgres as serving database, "
    "and Streamlit for the explorer UI."
)

st.subheader("Recent Blocks")
recent_blocks = get_recent_blocks()
if recent_blocks:
    st.dataframe([format_row_timestamps(row) for row in recent_blocks], use_container_width=True)
else:
    st.info("No block data yet. Run ETL first: `python etl/build_serving_tables.py`")

st.subheader("Recent Transactions")
recent_transactions = get_recent_transactions()
if recent_transactions:
    st.dataframe(
        [format_value_string_as_eth(format_row_timestamps(row)) for row in recent_transactions],
        use_container_width=True,
    )
else:
    st.info("No transaction data yet. Run ETL first: `python etl/build_serving_tables.py`")
