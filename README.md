# Ethereum Blockchain Explorer

An end-to-end Ethereum data engineering and analytics project that turns raw Parquet files into an indexed PostgreSQL serving layer and an interactive
Streamlit explorer.

The MVP focuses on the engineering challenges behind blockchain-scale analytics: bounded-memory ETL, parallel ingestion, idempotent backfills,
query-oriented data modelling, database performance, and a usable exploration interface.

![Ethereum Blockchain Explorer demo](videos/Demo_sample_1.gif)

## Project highlights

- End-to-end pipeline from raw Ethereum data to an interactive application
- Incremental, restart-safe ETL using a durable block checkpoint
- Parallel block-range ingestion with controlled CPU concurrency
- Direct Parquet querying and transformation with DuckDB
- Query-oriented PostgreSQL tables and indexes for explorer workloads
- Exact integer handling for Wei values and compact binary storage for hashes and addresses
- Multipage Streamlit interface with cached, parameterized database queries
- Time-series gas analytics with timeframe-dependent aggregation

## The problem

Blockchain datasets are large, append continuously, and are not naturally shaped for low-latency application queries. Reading the source Parquet files for
every user interaction would make searches and detail pages unnecessarily expensive.

This project separates ingestion from serving. DuckDB performs analytical scans and transformations close to the Parquet source, while PostgreSQL stores a
smaller, indexed model designed around the explorer's queries. Streamlit then reads only from that serving layer.

## Architecture

```mermaid
flowchart LR
    P[(Ethereum Parquet files)] -->|predicate-filtered scans| D[DuckDB ETL workers]
    D -->|transform and load| PG[(PostgreSQL)]
    PG --> Q[Python query layer]
    Q --> S[Streamlit explorer]
    M[(ingestion_meta)] -->|last completed block| D
    D -->|advance after all stages succeed| M
```

### Data flow

1. The ETL reads the latest completed block from PostgreSQL.
2. It discovers the highest block available in the Parquet dataset.
3. New blocks are divided into configurable block-number ranges.
4. Separate process pools load blocks, transactions, and address activity.
5. Transaction records are joined to blocks to add their timestamp.
6. Each transaction is expanded into sender and, where applicable, recipient activity rows.
7. The checkpoint advances only after all three serving tables finish loading successfully.

Conflict handling makes a retry safe: block records are upserted, while duplicate transaction and address-activity records are ignored.

## Features

### Explorer

- Dashboard of recent blocks and transactions
- Unified search for a block number, transaction hash, or address
- Block details and ordered transactions within a block
- Block discovery by day or date range, including average gas price
- Transaction details with ETH value, Gwei gas price, status, and calculated fee
- Address summary with first/latest activity and recent inbound/outbound transactions
- Gas-price trend chart with presets, custom ranges, and adaptive time buckets
- CET-aware timestamps and human-readable ETH/Gwei formatting

### Data pipeline

- Incremental ingestion based on the last successfully loaded block
- Configurable batch size, worker count, logging frequency, and DuckDB threads
- Parallel processing with an eight-core global concurrency cap
- Predicate filtering on block numbers to reduce unnecessary Parquet scans
- Progress and throughput reporting for each loading stage
- Idempotent writes that support recovery after an interrupted run

## Tech stack

| Layer | Technology | Why it is used |
| --- | --- | --- |
| Source | Parquet | Compact, columnar storage for historical blockchain data |
| Transformation | DuckDB | Efficient in-process Parquet scans, SQL transformations, and direct PostgreSQL loading |
| Serving | PostgreSQL 16 | Indexed, durable storage for predictable interactive queries |
| Application | Streamlit | Fast delivery of a multipage data product in Python |
| Data frames | Polars | Lightweight preparation of gas time-series results |
| Database client | Psycopg 3 | Parameterized PostgreSQL queries from the application |
| Environment | Docker Compose, `uv` | Reproducible database and Python dependency setup |

## Serving data model

| Table | Role | Key access patterns |
| --- | --- | --- |
| `blocks` | Canonical block metadata | Block lookup, newest blocks, date-range filtering |
| `tx` | Transaction facts enriched with block timestamps | Hash lookup, transactions by block, sender/recipient queries, gas aggregation |
| `address_tx` | Denormalized sender and recipient activity | Recent activity and first/latest block for an address |
| `ingestion_meta` | Pipeline checkpoint | Resume ingestion above the last fully completed block |

Ethereum hashes and addresses are stored as `BYTEA` instead of hexadecimal text to reduce row and index size. The original Wei value is retained as text
because Ethereum quantities can exceed conventional integer ranges; display conversions use Python's `Decimal` to avoid floating-point precision loss.

Indexes mirror the UI's access patterns, including transaction position within a block and reverse-chronological address activity. This improves read latency
at the cost of additional storage and bulk-write overhead.

## Repository structure

```text
.
├── app/
│   ├── Home.py                  # Recent blocks and transactions
│   └── pages/                   # Search, block, transaction, address, and gas views
├── etl/
│   └── build_serving_tables.py  # Schema creation and incremental ETL
├── lib/                         # Database, hex, time, value, and pagination helpers
├── videos/                      # Product demo
├── docker-compose.yml           # Local PostgreSQL service
├── pyproject.toml               # Python project and dependencies
└── uv.lock                      # Reproducible dependency lockfile
```

## Run locally

### Prerequisites

- Python 3.10 or newer
- [Docker and Docker Compose](https://docs.docker.com/compose/)
- [`uv`](https://docs.astral.sh/uv/)
- The Ethereum Parquet dataset described in the [source dataset documentation](https://huggingface.co/datasets/vnegi10/Ethereum_blockchain_parquet/blob/main/README.md)

The dataset directory must contain the following layout:

```text
Ethereum_blockchain_parquet/
├── blocks/
│   └── *.parquet
└── transactions/
    └── *.parquet
```

### 1. Install dependencies

```bash
uv sync
```

### 2. Configure PostgreSQL storage

The included Compose file persists PostgreSQL data at `/mnt/ugreen/postgres/eth_explorer_mvp`. Change that host path in `docker-compose.yml` if it does not
exist on your machine, then start the database:

```bash
docker compose up -d postgres
```

### 3. Configure the environment

Create a `.env` file in the repository root:

```dotenv
DATABASE_URL=postgresql://explorer:explorer@localhost:5433/explorer
PARQUET_DIR=/absolute/path/to/Ethereum_blockchain_parquet

# Optional ETL tuning; values below are the application defaults
ETL_BATCH_SIZE=50000
ETL_LOG_EVERY_BATCHES=10
ETL_WORKERS=2
DUCKDB_THREADS=8
```

`ETL_BATCH_SIZE` is a number of blocks, not a row count. Worker and DuckDB thread settings are capped so the ETL uses no more than eight cores in total.

On its first run, DuckDB may need to install its PostgreSQL extension.

### 4. Build or update the serving tables

```bash
uv run python etl/build_serving_tables.py
```

The first run backfills the available dataset. Later runs load only blocks above `ingestion_meta.last_ingested_block`.

### 5. Launch the explorer

```bash
uv run streamlit run app/Home.py
```

Open the address printed by Streamlit, normally <http://localhost:8501>.

## Operating the pipeline

Check the available serving data and ETL checkpoint:

```sql
SELECT COUNT(*) AS blocks, MAX(block_number) AS latest_block FROM blocks;

SELECT pipeline_name, last_ingested_block, updated_at
FROM ingestion_meta;
```

The checkpoint is intentionally updated only after the block, transaction, and address-activity stages have all completed. If execution stops earlier,
rerunning the ETL revisits that range and relies on the tables' conflict rules to prevent duplicate records.

To restart from a deliberate earlier point, update the checkpoint only after confirming that the corresponding serving-table data is in the desired state:

```sql
INSERT INTO ingestion_meta (pipeline_name, last_ingested_block, updated_at)
VALUES ('serving_tables_v1', 17199992, NOW())
ON CONFLICT (pipeline_name) DO UPDATE SET
  last_ingested_block = EXCLUDED.last_ingested_block,
  updated_at = NOW();
```

## Engineering decisions and trade-offs

- **Dedicated serving layer:** PostgreSQL avoids repeated full-dataset scans, but duplicates selected source fields and requires an ETL step.
- **Denormalized address activity:** Precomputing both sides of a transfer makes address pages simple and index-friendly, but can create two activity rows per transaction.
- **Parallel range loading:** Multiple processes increase throughput, while explicit thread caps prevent each DuckDB instance from oversubscribing the machine.
- **End-of-run checkpoint:** Advancing only after every table completes avoids marking a partial load as successful. A retry may repeat work, so inserts must remain idempotent.
- **Indexes for reads:** Purpose-built indexes improve explorer responsiveness but slow ingestion and consume disk space.
- **Streamlit delivery:** A Python-only UI keeps the MVP focused on data engineering. A separate API and frontend would provide finer control for a production
product.

## Current scope and roadmap

This repository is an MVP built around a local historical dataset. It is not a consensus client, wallet, or real-time chain indexer.

Potential next steps include:

- Add automated unit and integration tests for transformations, retry behavior, and query helpers
- Add CI checks for formatting, linting, and tests
- Replace the host-specific PostgreSQL bind mount with configurable storage
- Add container health checks and application-level database error handling
- Introduce migrations instead of creating the serving schema inside the ETL
- Add observability for ETL duration, failed ranges, database size, and query latency
- Support near-real-time ingestion from an Ethereum RPC endpoint
- Add an API and dedicated frontend for deployment at larger scale

## What this project demonstrates

For a reviewer, the repository provides concrete examples of:

- Data pipeline design and incremental processing
- Analytical SQL and data modelling for product requirements
- Parallelism, resource controls, idempotency, and recovery strategy
- PostgreSQL schema and index design
- Numeric precision and domain-aware data representation
- Translating backend datasets into an understandable end-user experience