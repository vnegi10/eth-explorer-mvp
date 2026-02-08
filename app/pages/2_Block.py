import sys
from datetime import datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from lib.db import fetch_all, fetch_one
from lib.time_utils import format_row_timestamps
from lib.value_utils import format_value_string_as_eth

CET = ZoneInfo("CET")


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

@st.cache_data(ttl=30)
def get_blocks_in_range(start_ts: int, end_ts: int, limit: int = 1000) -> list[dict]:
    return fetch_all(
        """
        SELECT
          b.block_number,
          b.timestamp,
          b.gas_used,
          b.base_fee_per_gas,
          b.chain_id,
          encode(b.block_hash, 'hex') AS block_hash_hex,
          encode(b.author, 'hex') AS author_hex,
          tx_stats.avg_gas_price_gwei
        FROM blocks b
        LEFT JOIN (
          SELECT
            block_number,
            AVG(gas_price)::DOUBLE PRECISION / 1e9 AS avg_gas_price_gwei
          FROM tx
          GROUP BY block_number
        ) AS tx_stats ON tx_stats.block_number = b.block_number
        WHERE b.timestamp BETWEEN %s AND %s
        ORDER BY b.block_number DESC
        LIMIT %s
        """,
        (start_ts, end_ts, limit),
    )


st.title("Block")
block_number = st.number_input("Block number", min_value=0, step=1, value=0, key="block_number")

block = get_block(int(block_number))
if block:
    st.subheader("Block Details")
    st.json(format_row_timestamps(block))

    st.subheader("Transactions")
    txs = get_block_txs(int(block_number))
    if txs:
        st.dataframe([format_value_string_as_eth(row) for row in txs], width="stretch")
    else:
        st.info("No transactions for this block.")
else:
    st.warning("Block not found.")

st.subheader("Blocks by Date")
mode = st.radio("Selection mode", ("Single day", "Date range"), horizontal=True, key="block_mode")
limit = st.number_input(
    "Max rows",
    min_value=10,
    max_value=20000,
    value=1000,
    step=10,
    key="block_rows_limit",
)

if mode == "Single day":
    day = st.date_input("Select day", key="block_single_day")
    start_dt = datetime.combine(day, time.min, tzinfo=CET)
    end_dt = datetime.combine(day, time.max, tzinfo=CET)
else:
    today = datetime.now(CET).date()
    default_start = today - timedelta(days=1)
    date_range = st.date_input("Select date range", value=(default_start, today), key="block_date_range")
    if not isinstance(date_range, tuple) or len(date_range) != 2:
        st.info("Select both a start and end date.")
        st.stop()
    start_date, end_date = date_range
    start_dt = datetime.combine(start_date, time.min, tzinfo=CET)
    end_dt = datetime.combine(end_date, time.max, tzinfo=CET)

start_ts = int(start_dt.timestamp())
end_ts = int(end_dt.timestamp())

blocks_in_range = get_blocks_in_range(start_ts, end_ts, int(limit))
if blocks_in_range:
    st.dataframe([format_row_timestamps(row) for row in blocks_in_range], width="stretch")
else:
    st.info("No blocks found in this date range.")
