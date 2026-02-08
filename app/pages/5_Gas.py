import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl
import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from lib.db import fetch_all


def timeframe_bounds(option: str, custom_start: datetime | None, custom_end: datetime | None) -> tuple[int, int]:
    now_utc = datetime.now(timezone.utc)
    if option == "Last 1 hour":
        start = now_utc - timedelta(hours=1)
    elif option == "Last 6 hours":
        start = now_utc - timedelta(hours=6)
    elif option == "Last 24 hours":
        start = now_utc - timedelta(hours=24)
    elif option == "Last 7 days":
        start = now_utc - timedelta(days=7)
    elif option == "Last 30 days":
        start = now_utc - timedelta(days=30)
    else:
        if custom_start is None or custom_end is None:
            raise ValueError("Custom timeframe requires start/end.")
        start = custom_start.astimezone(timezone.utc)
        now_utc = custom_end.astimezone(timezone.utc)
    return int(start.timestamp()), int(now_utc.timestamp())


def bucket_seconds(start_ts: int, end_ts: int) -> int:
    span = end_ts - start_ts
    if span <= 6 * 3600:
        return 60
    if span <= 24 * 3600:
        return 300
    if span <= 7 * 24 * 3600:
        return 900
    if span <= 30 * 24 * 3600:
        return 3600
    return 3 * 3600


@st.cache_data(ttl=60)
def get_gas_series(start_ts: int, end_ts: int, bucket_s: int) -> list[dict]:
    return fetch_all(
        """
        SELECT
          (block_timestamp / %s) * %s AS bucket_ts,
          AVG(gas_price)::DOUBLE PRECISION / 1e9 AS avg_gas_price_gwei,
          COUNT(*) AS tx_count
        FROM tx
        WHERE block_timestamp BETWEEN %s AND %s
        GROUP BY bucket_ts
        ORDER BY bucket_ts ASC
        """,
        (bucket_s, bucket_s, start_ts, end_ts),
    )


st.title("Gas")
st.caption("Gas price (Gwei) over time from Postgres transaction data.")

tf = st.selectbox(
    "Timeframe",
    ("Last 1 hour", "Last 6 hours", "Last 24 hours", "Last 7 days", "Last 30 days", "Custom"),
    index=2,
    key="gas_timeframe",
)

custom_start_dt = None
custom_end_dt = None
if tf == "Custom":
    now = datetime.now()
    c1, c2 = st.columns(2)
    with c1:
        start_date = st.date_input("Start date", value=(now - timedelta(days=1)).date(), key="gas_custom_start_date")
        start_time = st.time_input("Start time", value=datetime.min.time(), key="gas_custom_start_time")
    with c2:
        end_date = st.date_input("End date", value=now.date(), key="gas_custom_end_date")
        end_time = st.time_input("End time", value=now.time().replace(microsecond=0), key="gas_custom_end_time")
    custom_start_dt = datetime.combine(start_date, start_time).replace(tzinfo=timezone.utc)
    custom_end_dt = datetime.combine(end_date, end_time).replace(tzinfo=timezone.utc)
    if custom_end_dt <= custom_start_dt:
        st.error("End datetime must be after start datetime.")
        st.stop()

start_ts, end_ts = timeframe_bounds(tf, custom_start_dt, custom_end_dt)
bucket_s = bucket_seconds(start_ts, end_ts)
rows = get_gas_series(start_ts, end_ts, bucket_s)

if not rows:
    st.info("No data for this timeframe.")
    st.stop()

df = pl.DataFrame(rows).with_columns(
    pl.from_epoch("bucket_ts", time_unit="s")
    .dt.replace_time_zone("UTC")
    .dt.convert_time_zone("CET")
    .alias("block_timestamp")
)

st.line_chart(
    df,
    x="block_timestamp",
    y="avg_gas_price_gwei",
    width="stretch",
)

latest = float(df["avg_gas_price_gwei"][-1])
max_val = float(df["avg_gas_price_gwei"].max())
min_val = float(df["avg_gas_price_gwei"].min())
st.caption(
    f"Bucket: {bucket_s}s | points: {len(df)} | "
    f"latest: {latest:.2f} Gwei | min: {min_val:.2f} Gwei | max: {max_val:.2f} Gwei"
)
