# Eth Explorer MVP

A local-first Ethereum blockchain explorer built on top of Parquet data.

This project:
- backfills serving tables from Parquet into Postgres
- keeps data incrementally updated from a tracked last-ingested block
- exposes an interactive Streamlit UI for blocks, transactions, addresses, and gas analytics.

## Tech stack

- ETL/query engine: DuckDB
- Source data: Parquet files on local disk
- Serving database: Postgres (Docker)
- App/UI: Streamlit
- Python package/runtime management: uv

## Data source

Parquet dataset can be downloaded from HuggingFace:
- https://huggingface.co/datasets/vnegi10/Ethereum_blockchain_parquet/blob/main/README.md

## Features

- Home page with recent blocks and recent transactions
- Search page for block number, tx hash, or address
- Block page with:
  - block details
  - transactions for a block
  - blocks-by-date filter
  - average gas price (Gwei) per block in date-filter results
- Tx page with:
  - value shown in ETH
  - gas price shown in Gwei
  - estimated transaction fee in ETH
- Address page with summary + recent activity
- Gas page with timeframe-based gas-price trend (Gwei vs timestamp)

## Prerequisites

- Linux/macOS shell
- Python 3.10+
- Docker + Docker Compose
- uv

Install uv:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

## Project setup

From repo root:

1. Install dependencies
```bash
uv sync
```

2. Configure environment variables in `.env`
```env
DATABASE_URL=postgresql://explorer:explorer@localhost:5433/explorer
PARQUET_DIR=/absolute/path/to/Ethereum_blockchain_parquet

# Optional ETL tuning
ETL_BATCH_SIZE=100000
ETL_LOG_EVERY_BATCHES=10
ETL_WORKERS=2
DUCKDB_THREADS=8
```

3. Start Postgres
```bash
docker compose up -d postgres
```

## Run ETL (backfill/incremental)

```bash
uv run python etl/build_serving_tables.py
```

Notes:
- ETL tracks progress in `ingestion_meta.last_ingested_block`.
- Re-runs ingest only newer blocks by default.
- Progress logs print rows and throughput per batch.

## Run the app

```bash
uv run streamlit run app/Home.py
```

Open the URL printed by Streamlit (usually `http://localhost:8501`).

## Replication steps (end-to-end)

1. Clone this repository.
2. Download Ethereum Parquet data from HuggingFace.
3. Set `PARQUET_DIR` and `DATABASE_URL` in `.env`.
4. Start Postgres with Docker Compose.
5. Run ETL to backfill serving tables.
6. Start Streamlit and explore pages.

## Useful commands

Check latest block in Postgres:
```sql
SELECT MAX(block_number) AS latest_block FROM blocks;
```

Set ETL checkpoint manually (example):
```sql
INSERT INTO ingestion_meta (pipeline_name, last_ingested_block, updated_at)
VALUES ('serving_tables_v1', 17199992, NOW())
ON CONFLICT (pipeline_name)
DO UPDATE SET
  last_ingested_block = EXCLUDED.last_ingested_block,
  updated_at = NOW();
```

## Caveats

- Large initial backfills can take significant time and disk I/O.
- For best throughput, keep Parquet source and Postgres data on separate disks when possible.

## Demo

![Demo](videos/Demo_sample_1.gif)