import os
import time

import duckdb
from dotenv import load_dotenv

load_dotenv()

PARQUET_DIR = os.environ["PARQUET_DIR"]
DATABASE_URL = os.environ["DATABASE_URL"]
INGEST_PIPELINE = "serving_tables_v1"
ETL_BATCH_SIZE = int(os.environ.get("ETL_BATCH_SIZE", "100000"))
ETL_LOG_EVERY_BATCHES = int(os.environ.get("ETL_LOG_EVERY_BATCHES", "10"))
DUCKDB_THREADS = min(8, int(os.environ.get("DUCKDB_THREADS", "8")))

BLOCKS_GLOB = os.path.join(PARQUET_DIR, "blocks", "*.parquet")
TX_GLOB = os.path.join(PARQUET_DIR, "transactions", "*.parquet")


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
    if DUCKDB_THREADS <= 0:
        raise ValueError("DUCKDB_THREADS must be > 0")


def setup_duckdb() -> duckdb.DuckDBPyConnection:
    duck = duckdb.connect(database=":memory:")
    duck.execute(f"PRAGMA threads={DUCKDB_THREADS};")
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


def batch_load(
    duck: duckdb.DuckDBPyConnection,
    *,
    label: str,
    source_view: str,
    insert_sql_factory,
    first_block: int,
    last_block: int,
) -> None:
    total_rows = int(duck.execute(f"SELECT COUNT(*) FROM {source_view}").fetchone()[0])
    if total_rows == 0:
        print(f"[{label}] nothing to load")
        return

    total_batches = ((last_block - first_block) // ETL_BATCH_SIZE) + 1
    loaded_rows = 0
    started_at = time.perf_counter()

    for batch_idx, block_start in enumerate(range(first_block, last_block + 1, ETL_BATCH_SIZE), start=1):
        block_end = min(block_start + ETL_BATCH_SIZE - 1, last_block)
        batch_rows = int(
            duck.execute(
                f"""
                SELECT COUNT(*)
                FROM {source_view}
                WHERE block_number BETWEEN {block_start} AND {block_end}
                """
            ).fetchone()[0]
        )
        if batch_rows > 0:
            duck.execute(insert_sql_factory(block_start, block_end))
            loaded_rows += batch_rows

        if batch_idx % ETL_LOG_EVERY_BATCHES == 0 or batch_idx == total_batches:
            elapsed = time.perf_counter() - started_at
            rate = loaded_rows / elapsed if elapsed > 0 else 0.0
            print(
                f"[{label}] batch {batch_idx}/{total_batches}, "
                f"blocks {block_start}-{block_end}, "
                f"rows {loaded_rows:,}/{total_rows:,} ({rate:,.0f} rows/s)"
            )

    elapsed = time.perf_counter() - started_at
    rate = loaded_rows / elapsed if elapsed > 0 else 0.0
    print(f"[{label}] complete: {loaded_rows:,} rows in {elapsed:,.1f}s ({rate:,.0f} rows/s)")


def load_blocks(duck: duckdb.DuckDBPyConnection, first_block: int, last_block: int) -> None:
    def insert_sql(block_start: int, block_end: int) -> str:
        return f"""
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
        FROM v_blocks_new
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

    batch_load(
        duck,
        label="blocks",
        source_view="v_blocks_new",
        insert_sql_factory=insert_sql,
        first_block=first_block,
        last_block=last_block,
    )


def load_tx(duck: duckdb.DuckDBPyConnection, first_block: int, last_block: int) -> None:
    def insert_sql(block_start: int, block_end: int) -> str:
        return f"""
        INSERT INTO pg.tx (
          transaction_hash, block_number, transaction_index, nonce, from_address, to_address,
          value_string, value_f64, input, gas_limit, gas_used, gas_price, transaction_type,
          max_priority_fee_per_gas, max_fee_per_gas, success, n_input_bytes, n_input_zero_bytes,
          n_input_nonzero_bytes, chain_id, block_timestamp
        )
        SELECT
          transaction_hash,
          block_number::BIGINT,
          transaction_index::BIGINT,
          nonce::BIGINT,
          from_address,
          NULLIF(to_address, ''::BLOB) AS to_address,
          value_string,
          value_f64,
          input,
          gas_limit::BIGINT,
          gas_used::BIGINT,
          gas_price::BIGINT,
          transaction_type::BIGINT,
          max_priority_fee_per_gas::BIGINT,
          max_fee_per_gas::BIGINT,
          success,
          n_input_bytes::BIGINT,
          n_input_zero_bytes::BIGINT,
          n_input_nonzero_bytes::BIGINT,
          chain_id::BIGINT,
          block_timestamp::BIGINT
        FROM v_tx_new
        WHERE block_number BETWEEN {block_start} AND {block_end}
        ON CONFLICT (transaction_hash) DO NOTHING
        """

    batch_load(
        duck,
        label="tx",
        source_view="v_tx_new",
        insert_sql_factory=insert_sql,
        first_block=first_block,
        last_block=last_block,
    )


def load_address_tx(duck: duckdb.DuckDBPyConnection, first_block: int, last_block: int) -> None:
    def insert_sql(block_start: int, block_end: int) -> str:
        return f"""
        INSERT INTO pg.address_tx (
          address, transaction_hash, block_number, transaction_index, direction, counterparty,
          value_string, value_f64, success, block_timestamp, chain_id
        )
        SELECT
          from_address AS address,
          transaction_hash,
          block_number::BIGINT,
          transaction_index::BIGINT,
          0::SMALLINT AS direction,
          NULLIF(to_address, ''::BLOB) AS counterparty,
          value_string,
          value_f64,
          success,
          block_timestamp::BIGINT,
          chain_id::BIGINT
        FROM v_tx_new
        WHERE block_number BETWEEN {block_start} AND {block_end}
        UNION ALL
        SELECT
          NULLIF(to_address, ''::BLOB) AS address,
          transaction_hash,
          block_number::BIGINT,
          transaction_index::BIGINT,
          1::SMALLINT AS direction,
          from_address AS counterparty,
          value_string,
          value_f64,
          success,
          block_timestamp::BIGINT,
          chain_id::BIGINT
        FROM v_tx_new
        WHERE block_number BETWEEN {block_start} AND {block_end}
          AND NULLIF(to_address, ''::BLOB) IS NOT NULL
        ON CONFLICT (address, block_number, transaction_index, transaction_hash, direction) DO NOTHING
        """

    batch_load(
        duck,
        label="address_tx",
        source_view="v_tx_new",
        insert_sql_factory=insert_sql,
        first_block=first_block,
        last_block=last_block,
    )


def main() -> None:
    validate_settings()
    duck = setup_duckdb()

    duck.execute(DDL)
    duck.execute(f"CREATE OR REPLACE VIEW v_blocks_all AS SELECT * FROM read_parquet('{BLOCKS_GLOB}');")
    duck.execute(f"CREATE OR REPLACE VIEW v_tx_raw_all AS SELECT * FROM read_parquet('{TX_GLOB}');")

    last_ingested_block = get_last_ingested_block(duck)
    max_available_block = int(
        duck.execute("SELECT COALESCE(MAX(block_number), 0)::BIGINT FROM v_blocks_all").fetchone()[0]
    )

    if max_available_block <= last_ingested_block:
        print(
            f"No new blocks. last_ingested_block={last_ingested_block}, "
            f"max_available_block={max_available_block}"
        )
        return

    first_block = last_ingested_block + 1
    duck.execute(
        f"CREATE OR REPLACE VIEW v_blocks_new AS SELECT * FROM v_blocks_all WHERE block_number >= {first_block};"
    )
    duck.execute(
        f"CREATE OR REPLACE VIEW v_tx_raw_new AS SELECT * FROM v_tx_raw_all WHERE block_number >= {first_block};"
    )
    duck.execute(
        """
        CREATE OR REPLACE VIEW v_tx_new AS
        SELECT
          t.transaction_hash,
          t.block_number,
          t.transaction_index,
          t.nonce,
          t.from_address,
          t.to_address,
          t.value_string,
          t.value_f64,
          t.input,
          t.gas_limit,
          t.gas_used,
          t.gas_price,
          t.transaction_type,
          t.max_priority_fee_per_gas,
          t.max_fee_per_gas,
          t.success,
          t.n_input_bytes,
          t.n_input_zero_bytes,
          t.n_input_nonzero_bytes,
          t.chain_id,
          b.timestamp AS block_timestamp
        FROM v_tx_raw_new t
        JOIN v_blocks_new b USING (block_number)
        """
    )

    new_blocks = int(duck.execute("SELECT COUNT(*) FROM v_blocks_new").fetchone()[0])
    new_tx = int(duck.execute("SELECT COUNT(*) FROM v_tx_new").fetchone()[0])
    print(
        f"Loading new data: blocks={new_blocks}, tx={new_tx}, "
        f"from_block>{last_ingested_block}..{max_available_block}, "
        f"batch_size={ETL_BATCH_SIZE}, log_every_batches={ETL_LOG_EVERY_BATCHES}, threads={DUCKDB_THREADS}"
    )

    load_blocks(duck, first_block, max_available_block)
    load_tx(duck, first_block, max_available_block)
    load_address_tx(duck, first_block, max_available_block)
    set_last_ingested_block(duck, max_available_block)

    print("Serving tables updated incrementally via DuckDB Postgres extension.")


if __name__ == "__main__":
    main()
