"""Main Streamlit entrypoint for the ETH explorer MVP.

All interactive data on this UI comes from Postgres serving tables.
"""

import sys
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from lib.db import fetch_all


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


st.set_page_config(page_title="ETH Explorer MVP", layout="wide")
st.title("ETH Explorer MVP")
st.caption("Postgres-backed explorer MVP. Use the sidebar to navigate pages.")

st.subheader("Recent Blocks")
recent_blocks = get_recent_blocks()
if recent_blocks:
    cet = ZoneInfo("CET")
    for row in recent_blocks:
        ts = row.get("timestamp")
        if isinstance(ts, int):
            row["timestamp"] = datetime.fromtimestamp(ts, tz=cet).strftime("%Y-%m-%d %H:%M:%S %Z")
        else:
            row["timestamp"] = None
    st.dataframe(recent_blocks, use_container_width=True)
else:
    st.info("No block data yet. Run ETL first: `python etl/build_serving_tables.py`")
