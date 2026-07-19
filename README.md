# Ethereum Blockchain Explorer (MVP)

A full-stack Ethereum analytics project that transforms large Parquet datasets into indexed PostgreSQL serving tables and exposes them through an interactive
Streamlit explorer.

The project focuses on the engineering challenges behind blockchain-scale analytics: bounded-memory ETL, parallel ingestion, idempotent backfills,
query-oriented data modelling, database performance, and a usable exploration interface.

## What This Project Demonstrates

- Building a complete analytical data path from raw files to a user-facing application
- Processing tens of millions of Ethereum records with DuckDB and Python
- Designing resumable, configurable ETL jobs for large historical backfills
- Optimizing Parquet scans with timestamp windows and block-number predicate pruning
- Serving low-latency application queries from PostgreSQL with purpose-built indexes
- Operating PostgreSQL in Docker with bulk-ingest tuning and persistent storage
- Presenting blockchain data through search, detail views, address activity, and time-series analytics

## Features

- Recent Ethereum blocks and transactions dashboard
- Unified search by block number, transaction hash, or address
- Block details with transactions and date-range exploration
- Transaction details, including value, gas price, and calculated transaction fee
- Address summaries with first/latest activity and recent inbound/outbound transactions
- Gas-price time series with adaptive aggregation buckets and custom date ranges
- CET-aware timestamp display and Wei-to-ETH/Gwei formatting
- Cached Streamlit queries to reduce repeated database load

## Architecture

```mermaid
flowchart LR
    A[Ethereum Parquet files] --> B[DuckDB ETL workers]
    B --> C[(PostgreSQL serving tables)]
    C --> D[Python query layer]
    D --> E[Streamlit explorer]

    F[Date window and chunk settings] --> B
    G[Ingestion metadata] <--> B
```

The system deliberately separates analytical ingestion from application serving:

- **Parquet** is the compact source format for historical blockchain data.
- **DuckDB** scans and transforms Parquet directly without loading entire datasets into Python memory.
- **PostgreSQL** stores application-oriented tables and indexes for predictable interactive queries.
- **Streamlit** provides a multipage explorer backed exclusively by the PostgreSQL serving layer.

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

## ETL Design

The ETL entry point is [`etl/build_serving_tables.py`](etl/build_serving_tables.py).

### Bounded-memory processing

The requested date window is divided into configurable chunks. `ETL_CHUNK_DAYS` accepts whole or fractional values, so `0.5` represents 12 hours. Each worker processes only one chunk at a time.

### Parallel execution

`ProcessPoolExecutor` schedules one task per time range while limiting active work to `ETL_WORKERS`. DuckDB threads are capped per worker so the process pool does not oversubscribe the host.

### Parquet pruning

For transaction workloads, the ETL first identifies the minimum and maximum block numbers in the timestamp-filtered block set. It then applies that range to the transaction Parquet scan before performing an exact inner join to the selected blocks. This reduces data scanned while preserving timestamp-window correctness.

### Idempotency and recovery

- Blocks use `ON CONFLICT ... DO UPDATE`.
- Transactions and address activity use `ON CONFLICT ... DO NOTHING`.
- `ingestion_window_meta` records `in_progress`, `completed`, and `failed` states.
- Completed windows are skipped unless `ETL_FORCE_RELOAD_WINDOW=1` is set.
- Each completed batch is committed independently, allowing interrupted runs to restart safely.

### Progress reporting

Each loading stage reports completed batches, cumulative rows, elapsed time, and throughput. Blocks, transactions, and address activity are loaded as separate stages.

## Data Model

| Table | Purpose | Important keys/indexes |
| --- | --- | --- |
| `blocks` | Canonical block metadata | Primary key on `block_number`; descending block index |
| `tx` | Transaction details and block timestamp | Primary key on transaction hash; unique block position; block/from/to lookup indexes |
| `address_tx` | Address-centric inbound/outbound activity | Composite primary key; recent activity index by address and block position |
| `ingestion_window_meta` | ETL window state and rerun control | Composite key on pipeline and window timestamps |

Ethereum hashes and addresses are stored as binary values (`BYTEA`) rather than text, reducing storage and index size. Large Wei values are retained as strings where necessary to avoid integer overflow or precision loss.

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

## Design Trade-offs

- PostgreSQL indexes improve explorer latency but increase bulk-ingest cost.
- Per-batch commits make interrupted runs recoverable but do not provide whole-window atomicity.
- Exact block joins preserve correctness after range pruning, at the cost of an additional join.
- Streamlit provides rapid product delivery, while a dedicated API/frontend would offer more control for a production deployment.
- The current direct-to-serving-table approach is simple; staging tables and deferred index creation could improve very large backfills.

## Demo

![Demo](videos/Demo_sample_1.gif)