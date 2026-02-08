import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed

import duckdb
from dotenv import load_dotenv

load_dotenv()

PARQUET_DIR = os.environ["PARQUET_DIR"]
DATABASE_URL = os.environ["DATABASE_URL"]
INGEST_PIPELINE = "serving_tables_v1"
ETL_BATCH_SIZE = int(os.environ.get("ETL_BATCH_SIZE", "100000"))
ETL_LOG_EVERY_BATCHES = int(os.environ.get("ETL_LOG_EVERY_BATCHES", "10"))
ETL_WORKERS = min(8, int(os.environ.get("ETL_WORKERS", "4")))
DUCKDB_THREADS = min(8, int(os.environ.get("DUCKDB_THREADS", "8")))
MAX_TOTAL_CORES = 8

BLOCKS_GLOB = os.path.join(PARQUET_DIR, "blocks", "*.parquet")
TX_GLOB = os.path.join(PARQUET_DIR, "transactions", "*.parquet")
BLOCKS_GLOB_SQL = BLOCKS_GLOB.replace("'", "''")
TX_GLOB_SQL = TX_GLOB.replace("'", "''")


DDL = """
CREATE TABLE IF NOT EXISTS pg.ingestion_meta (
  pipeline_name TEXT PRIMARY KEY,
  last_ingested_block BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pg.blocks (
  block_number BIGINT PRIMARY KEY,
  block_hash BYTEA NOT NULL,
  author BYTEA,
  gas_used BIGINT,
  extra_data BYTEA,
  timestamp BIGINT,
  base_fee_per_gas BIGINT,
  chain_id BIGINT
);
CREATE INDEX IF NOT EXISTS blocks_number_desc ON pg.blocks (block_number DESC);

CREATE TABLE IF NOT EXISTS pg.tx (
  transaction_hash BYTEA PRIMARY KEY,
  block_number BIGINT NOT NULL,
  transaction_index BIGINT NOT NULL,
  nonce BIGINT,
  from_address BYTEA NOT NULL,
  to_address BYTEA,
  value_string TEXT,
  value_f64 DOUBLE PRECISION,
  input BYTEA,
  gas_limit BIGINT,
  gas_used BIGINT,
  gas_price BIGINT,
  transaction_type BIGINT,
  max_priority_fee_per_gas BIGINT,
  max_fee_per_gas BIGINT,
  success BOOLEAN,
  n_input_bytes BIGINT,
  n_input_zero_bytes BIGINT,
  n_input_nonzero_bytes BIGINT,
  chain_id BIGINT,
  block_timestamp BIGINT
);
CREATE UNIQUE INDEX IF NOT EXISTS tx_block_pos ON pg.tx (block_number, transaction_index);
CREATE INDEX IF NOT EXISTS tx_block_desc ON pg.tx (block_number DESC, transaction_index DESC);
CREATE INDEX IF NOT EXISTS tx_from_desc ON pg.tx (from_address, block_number DESC, transaction_index DESC);
CREATE INDEX IF NOT EXISTS tx_to_desc ON pg.tx (to_address, block_number DESC, transaction_index DESC);

CREATE TABLE IF NOT EXISTS pg.address_tx (
  address BYTEA NOT NULL,
  transaction_hash BYTEA NOT NULL,
  block_number BIGINT NOT NULL,
  transaction_index BIGINT NOT NULL,
  direction SMALLINT NOT NULL,
  counterparty BYTEA,
  value_string TEXT,
  value_f64 DOUBLE PRECISION,
  success BOOLEAN,
  block_timestamp BIGINT,
  chain_id BIGINT,
  PRIMARY KEY (address, block_number, transaction_index, transaction_hash, direction)
);
CREATE INDEX IF NOT EXISTS address_tx_recent
  ON pg.address_tx (address, block_number DESC, transaction_index DESC);
"""


def validate_settings() -> None:
    if ETL_BATCH_SIZE <= 0:
        raise ValueError("ETL_BATCH_SIZE must be > 0")
    if ETL_LOG_EVERY_BATCHES <= 0:
        raise ValueError("ETL_LOG_EVERY_BATCHES must be > 0")
    if ETL_WORKERS <= 0:
        raise ValueError("ETL_WORKERS must be > 0")
    if DUCKDB_THREADS <= 0:
        raise ValueError("DUCKDB_THREADS must be > 0")


def threads_per_worker() -> int:
    cap_by_workers = max(1, MAX_TOTAL_CORES // ETL_WORKERS)
    return max(1, min(DUCKDB_THREADS, cap_by_workers))


def setup_duckdb() -> duckdb.DuckDBPyConnection:
    duck = duckdb.connect(database=":memory:")
    duck.execute(f"PRAGMA threads={threads_per_worker()};")
    try:
        duck.execute("LOAD postgres;")
    except duckdb.Error:
        duck.execute("INSTALL postgres;")
        duck.execute("LOAD postgres;")

    conn = DATABASE_URL.replace("'", "''")
    duck.execute(f"ATTACH '{conn}' AS pg (TYPE postgres);")
    return duck


def get_last_ingested_block(duck: duckdb.DuckDBPyConnection) -> int:
    row = duck.execute(
        """
        SELECT COALESCE(
          (SELECT last_ingested_block
           FROM pg.ingestion_meta
           WHERE pipeline_name = ?),
          0
        )::BIGINT
        """,
        [INGEST_PIPELINE],
    ).fetchone()
    return int(row[0])


def set_last_ingested_block(duck: duckdb.DuckDBPyConnection, block_number: int) -> None:
    duck.execute(
        """
        INSERT INTO pg.ingestion_meta (pipeline_name, last_ingested_block, updated_at)
        VALUES (?, ?, NOW())
        ON CONFLICT (pipeline_name) DO UPDATE SET
          last_ingested_block = EXCLUDED.last_ingested_block,
          updated_at = NOW()
        """,
        [INGEST_PIPELINE, block_number],
    )


def build_ranges(first_block: int, last_block: int) -> list[tuple[int, int]]:
    return [
        (start, min(start + ETL_BATCH_SIZE - 1, last_block))
        for start in range(first_block, last_block + 1, ETL_BATCH_SIZE)
    ]


def _worker_load_blocks(block_start: int, block_end: int) -> tuple[int, int, int]:
    duck = setup_duckdb()
    try:
        count = int(
            duck.execute(
                f"""
                SELECT COUNT(*)
                FROM read_parquet('{BLOCKS_GLOB_SQL}')
                WHERE block_number BETWEEN {block_start} AND {block_end}
                """
            ).fetchone()[0]
        )
        if count > 0:
            duck.execute(
                f"""
                INSERT INTO pg.blocks (
                  block_number, block_hash, author, gas_used, extra_data, timestamp, base_fee_per_gas, chain_id
                )
                SELECT
                  block_number::BIGINT,
                  block_hash,
                  author,
                  gas_used::BIGINT,
                  extra_data,
                  timestamp::BIGINT,
                  base_fee_per_gas::BIGINT,
                  chain_id::BIGINT
                FROM read_parquet('{BLOCKS_GLOB_SQL}')
                WHERE block_number BETWEEN {block_start} AND {block_end}
                ON CONFLICT (block_number) DO UPDATE SET
                  block_hash = EXCLUDED.block_hash,
                  author = EXCLUDED.author,
                  gas_used = EXCLUDED.gas_used,
                  extra_data = EXCLUDED.extra_data,
                  timestamp = EXCLUDED.timestamp,
                  base_fee_per_gas = EXCLUDED.base_fee_per_gas,
                  chain_id = EXCLUDED.chain_id
                """
            )
        return (block_start, block_end, count)
    finally:
        duck.close()


def _worker_load_tx(block_start: int, block_end: int) -> tuple[int, int, int]:
    duck = setup_duckdb()
    try:
        count = int(
            duck.execute(
                f"""
                WITH blocks AS (
                  SELECT block_number, timestamp
                  FROM read_parquet('{BLOCKS_GLOB_SQL}')
                  WHERE block_number BETWEEN {block_start} AND {block_end}
                ),
                tx AS (
                  SELECT *
                  FROM read_parquet('{TX_GLOB_SQL}')
                  WHERE block_number BETWEEN {block_start} AND {block_end}
                )
                SELECT COUNT(*)
                FROM tx
                JOIN blocks USING (block_number)
                """
            ).fetchone()[0]
        )
        if count > 0:
            duck.execute(
                f"""
                WITH blocks AS (
                  SELECT block_number, timestamp
                  FROM read_parquet('{BLOCKS_GLOB_SQL}')
                  WHERE block_number BETWEEN {block_start} AND {block_end}
                ),
                tx AS (
                  SELECT *
                  FROM read_parquet('{TX_GLOB_SQL}')
                  WHERE block_number BETWEEN {block_start} AND {block_end}
                )
                INSERT INTO pg.tx (
                  transaction_hash, block_number, transaction_index, nonce, from_address, to_address,
                  value_string, value_f64, input, gas_limit, gas_used, gas_price, transaction_type,
                  max_priority_fee_per_gas, max_fee_per_gas, success, n_input_bytes, n_input_zero_bytes,
                  n_input_nonzero_bytes, chain_id, block_timestamp
                )
                SELECT
                  t.transaction_hash,
                  t.block_number::BIGINT,
                  t.transaction_index::BIGINT,
                  t.nonce::BIGINT,
                  t.from_address,
                  NULLIF(t.to_address, ''::BLOB) AS to_address,
                  t.value_string,
                  t.value_f64,
                  t.input,
                  t.gas_limit::BIGINT,
                  t.gas_used::BIGINT,
                  t.gas_price::BIGINT,
                  t.transaction_type::BIGINT,
                  t.max_priority_fee_per_gas::BIGINT,
                  t.max_fee_per_gas::BIGINT,
                  t.success,
                  t.n_input_bytes::BIGINT,
                  t.n_input_zero_bytes::BIGINT,
                  t.n_input_nonzero_bytes::BIGINT,
                  t.chain_id::BIGINT,
                  b.timestamp::BIGINT AS block_timestamp
                FROM tx t
                JOIN blocks b USING (block_number)
                ON CONFLICT (transaction_hash) DO NOTHING
                """
            )
        return (block_start, block_end, count)
    finally:
        duck.close()


def _worker_load_address_tx(block_start: int, block_end: int) -> tuple[int, int, int]:
    duck = setup_duckdb()
    try:
        count = int(
            duck.execute(
                f"""
                WITH blocks AS (
                  SELECT block_number, timestamp
                  FROM read_parquet('{BLOCKS_GLOB_SQL}')
                  WHERE block_number BETWEEN {block_start} AND {block_end}
                ),
                tx AS (
                  SELECT *
                  FROM read_parquet('{TX_GLOB_SQL}')
                  WHERE block_number BETWEEN {block_start} AND {block_end}
                ),
                joined_tx AS (
                  SELECT
                    t.transaction_hash,
                    t.block_number,
                    t.transaction_index,
                    t.from_address,
                    NULLIF(t.to_address, ''::BLOB) AS to_address,
                    t.value_string,
                    t.value_f64,
                    t.success,
                    t.chain_id,
                    b.timestamp AS block_timestamp
                  FROM tx t
                  JOIN blocks b USING (block_number)
                )
                SELECT
                  COUNT(*) + SUM(CASE WHEN to_address IS NOT NULL THEN 1 ELSE 0 END)
                FROM joined_tx
                """
            ).fetchone()[0]
        )
        count = int(count or 0)
        if count > 0:
            duck.execute(
                f"""
                WITH blocks AS (
                  SELECT block_number, timestamp
                  FROM read_parquet('{BLOCKS_GLOB_SQL}')
                  WHERE block_number BETWEEN {block_start} AND {block_end}
                ),
                tx AS (
                  SELECT *
                  FROM read_parquet('{TX_GLOB_SQL}')
                  WHERE block_number BETWEEN {block_start} AND {block_end}
                ),
                joined_tx AS (
                  SELECT
                    t.transaction_hash,
                    t.block_number::BIGINT AS block_number,
                    t.transaction_index::BIGINT AS transaction_index,
                    t.from_address,
                    NULLIF(t.to_address, ''::BLOB) AS to_address,
                    t.value_string,
                    t.value_f64,
                    t.success,
                    t.chain_id::BIGINT AS chain_id,
                    b.timestamp::BIGINT AS block_timestamp
                  FROM tx t
                  JOIN blocks b USING (block_number)
                )
                INSERT INTO pg.address_tx (
                  address, transaction_hash, block_number, transaction_index, direction, counterparty,
                  value_string, value_f64, success, block_timestamp, chain_id
                )
                SELECT
                  from_address AS address,
                  transaction_hash,
                  block_number,
                  transaction_index,
                  0::SMALLINT AS direction,
                  to_address AS counterparty,
                  value_string,
                  value_f64,
                  success,
                  block_timestamp,
                  chain_id
                FROM joined_tx
                UNION ALL
                SELECT
                  to_address AS address,
                  transaction_hash,
                  block_number,
                  transaction_index,
                  1::SMALLINT AS direction,
                  from_address AS counterparty,
                  value_string,
                  value_f64,
                  success,
                  block_timestamp,
                  chain_id
                FROM joined_tx
                WHERE to_address IS NOT NULL
                ON CONFLICT (address, block_number, transaction_index, transaction_hash, direction) DO NOTHING
                """
            )
        return (block_start, block_end, count)
    finally:
        duck.close()


def run_parallel_load(
    *,
    label: str,
    ranges: list[tuple[int, int]],
    total_rows: int,
    worker_fn,
) -> None:
    if total_rows == 0:
        print(f"[{label}] nothing to load")
        return

    started_at = time.perf_counter()
    loaded_rows = 0
    completed = 0
    total_batches = len(ranges)

    with ProcessPoolExecutor(max_workers=ETL_WORKERS) as executor:
        futures = [executor.submit(worker_fn, start, end) for start, end in ranges]
        for future in as_completed(futures):
            block_start, block_end, batch_rows = future.result()
            loaded_rows += batch_rows
            completed += 1
            if completed % ETL_LOG_EVERY_BATCHES == 0 or completed == total_batches:
                elapsed = time.perf_counter() - started_at
                rate = loaded_rows / elapsed if elapsed > 0 else 0.0
                print(
                    f"[{label}] batch {completed}/{total_batches}, "
                    f"blocks {block_start}-{block_end}, "
                    f"rows {loaded_rows:,}/{total_rows:,} ({rate:,.0f} rows/s)"
                )

    elapsed = time.perf_counter() - started_at
    rate = loaded_rows / elapsed if elapsed > 0 else 0.0
    print(f"[{label}] complete: {loaded_rows:,} rows in {elapsed:,.1f}s ({rate:,.0f} rows/s)")


def main() -> None:
    validate_settings()
    duck = setup_duckdb()
    try:
        duck.execute(DDL)

        last_ingested_block = get_last_ingested_block(duck)
        max_available_block = int(
            duck.execute(
                f"SELECT COALESCE(MAX(block_number), 0)::BIGINT FROM read_parquet('{BLOCKS_GLOB_SQL}')"
            ).fetchone()[0]
        )

        if max_available_block <= last_ingested_block:
            print(
                f"No new blocks. last_ingested_block={last_ingested_block}, "
                f"max_available_block={max_available_block}"
            )
            return

        first_block = last_ingested_block + 1
        ranges = build_ranges(first_block, max_available_block)

        new_blocks = int(
            duck.execute(
                f"""
                SELECT COUNT(*)
                FROM read_parquet('{BLOCKS_GLOB_SQL}')
                WHERE block_number >= {first_block}
                """
            ).fetchone()[0]
        )
        new_tx = int(
            duck.execute(
                f"""
                WITH blocks AS (
                  SELECT block_number
                  FROM read_parquet('{BLOCKS_GLOB_SQL}')
                  WHERE block_number >= {first_block}
                ),
                tx AS (
                  SELECT block_number
                  FROM read_parquet('{TX_GLOB_SQL}')
                  WHERE block_number >= {first_block}
                )
                SELECT COUNT(*)
                FROM tx
                JOIN blocks USING (block_number)
                """
            ).fetchone()[0]
        )
        new_address_tx = int(
            duck.execute(
                f"""
                WITH blocks AS (
                  SELECT block_number
                  FROM read_parquet('{BLOCKS_GLOB_SQL}')
                  WHERE block_number >= {first_block}
                ),
                tx AS (
                  SELECT block_number, NULLIF(to_address, ''::BLOB) AS to_address
                  FROM read_parquet('{TX_GLOB_SQL}')
                  WHERE block_number >= {first_block}
                ),
                joined_tx AS (
                  SELECT tx.to_address
                  FROM tx
                  JOIN blocks USING (block_number)
                )
                SELECT
                  COUNT(*) + SUM(CASE WHEN to_address IS NOT NULL THEN 1 ELSE 0 END)
                FROM joined_tx
                """
            ).fetchone()[0]
            or 0
        )

        print(
            f"Loading new data: blocks={new_blocks}, tx={new_tx}, address_tx={new_address_tx}, "
            f"from_block>{last_ingested_block}..{max_available_block}, "
            f"batch_size={ETL_BATCH_SIZE}, workers={ETL_WORKERS}, "
            f"log_every_batches={ETL_LOG_EVERY_BATCHES}, "
            f"threads_per_worker={threads_per_worker()} (max_total_cores={MAX_TOTAL_CORES})"
        )

        run_parallel_load(
            label="blocks",
            ranges=ranges,
            total_rows=new_blocks,
            worker_fn=_worker_load_blocks,
        )
        run_parallel_load(
            label="tx",
            ranges=ranges,
            total_rows=new_tx,
            worker_fn=_worker_load_tx,
        )
        run_parallel_load(
            label="address_tx",
            ranges=ranges,
            total_rows=new_address_tx,
            worker_fn=_worker_load_address_tx,
        )

        # Checkpoint moves only after all workers complete across all tables.
        set_last_ingested_block(duck, max_available_block)
        print("Serving tables updated incrementally via multi-worker DuckDB Postgres extension.")
    finally:
        duck.close()


if __name__ == "__main__":
    main()
