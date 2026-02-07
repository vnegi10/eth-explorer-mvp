import os
import time

import duckdb
import psycopg
from dotenv import load_dotenv
from psycopg import sql

load_dotenv()

PARQUET_DIR = os.environ["PARQUET_DIR"]
DATABASE_URL = os.environ["DATABASE_URL"]
INGEST_PIPELINE = "serving_tables_v1"
ETL_BATCH_SIZE = int(os.environ.get("ETL_BATCH_SIZE", "100000"))
ETL_LOG_EVERY_BATCHES = int(os.environ.get("ETL_LOG_EVERY_BATCHES", "10"))

BLOCKS_GLOB = os.path.join(PARQUET_DIR, "blocks", "*.parquet")
TX_GLOB = os.path.join(PARQUET_DIR, "transactions", "*.parquet")


DDL = """
CREATE TABLE IF NOT EXISTS ingestion_meta (
  pipeline_name TEXT PRIMARY KEY,
  last_ingested_block BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS blocks (
  block_number BIGINT PRIMARY KEY,
  block_hash BYTEA NOT NULL,
  author BYTEA,
  gas_used BIGINT,
  extra_data BYTEA,
  timestamp BIGINT,
  base_fee_per_gas BIGINT,
  chain_id BIGINT
);
CREATE INDEX IF NOT EXISTS blocks_number_desc ON blocks (block_number DESC);

CREATE TABLE IF NOT EXISTS tx (
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
CREATE UNIQUE INDEX IF NOT EXISTS tx_block_pos ON tx (block_number, transaction_index);
CREATE INDEX IF NOT EXISTS tx_block_desc ON tx (block_number DESC, transaction_index DESC);
CREATE INDEX IF NOT EXISTS tx_from_desc ON tx (from_address, block_number DESC, transaction_index DESC);
CREATE INDEX IF NOT EXISTS tx_to_desc   ON tx (to_address,   block_number DESC, transaction_index DESC);

CREATE TABLE IF NOT EXISTS address_tx (
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
  ON address_tx (address, block_number DESC, transaction_index DESC);
"""


def copy_query_to_table(
    cur: psycopg.Cursor,
    duck: duckdb.DuckDBPyConnection,
    query: str,
    table_name: str,
    columns: list[str],
    batch_size: int = ETL_BATCH_SIZE,
    log_every_batches: int = ETL_LOG_EVERY_BATCHES,
    label: str | None = None,
) -> None:
    if batch_size <= 0:
        raise ValueError("ETL_BATCH_SIZE must be > 0")
    if log_every_batches <= 0:
        raise ValueError("ETL_LOG_EVERY_BATCHES must be > 0")

    duck.execute(query)
    table_label = label or table_name
    started_at = time.perf_counter()
    total_rows = 0
    batch_num = 0
    with cur.copy(
        sql.SQL("COPY {} ({}) FROM STDIN").format(
            sql.Identifier(table_name),
            sql.SQL(", ").join(map(sql.Identifier, columns)),
        )
    ) as copy:
        while True:
            rows = duck.fetchmany(batch_size)
            if not rows:
                break
            batch_num += 1
            batch_count = len(rows)
            total_rows += batch_count
            for row in rows:
                copy.write_row(row)
            if batch_num % log_every_batches == 0:
                elapsed = time.perf_counter() - started_at
                rate = total_rows / elapsed if elapsed > 0 else 0.0
                print(
                    f"[{table_label}] copied {total_rows:,} rows "
                    f"across {batch_num} batches ({rate:,.0f} rows/s)"
                )
    elapsed = time.perf_counter() - started_at
    rate = total_rows / elapsed if elapsed > 0 else 0.0
    print(
        f"[{table_label}] copy complete: {total_rows:,} rows "
        f"in {elapsed:,.1f}s ({rate:,.0f} rows/s)"
    )


def get_last_ingested_block(cur: psycopg.Cursor, pipeline_name: str) -> int:
    cur.execute(
        """
        SELECT last_ingested_block
        FROM ingestion_meta
        WHERE pipeline_name = %s
        """,
        (pipeline_name,),
    )
    row = cur.fetchone()
    return int(row[0]) if row else 0


def set_last_ingested_block(cur: psycopg.Cursor, pipeline_name: str, block_number: int) -> None:
    cur.execute(
        """
        INSERT INTO ingestion_meta (pipeline_name, last_ingested_block, updated_at)
        VALUES (%s, %s, NOW())
        ON CONFLICT (pipeline_name)
        DO UPDATE SET
          last_ingested_block = EXCLUDED.last_ingested_block,
          updated_at = NOW()
        """,
        (pipeline_name, block_number),
    )


def load_blocks(cur: psycopg.Cursor, duck: duckdb.DuckDBPyConnection) -> None:
    columns = [
        "block_number",
        "block_hash_hex",
        "author_hex",
        "gas_used",
        "extra_data_hex",
        "timestamp",
        "base_fee_per_gas",
        "chain_id",
    ]
    query = """
      SELECT
        block_number::BIGINT,
        hex(block_hash) AS block_hash_hex,
        hex(author) AS author_hex,
        gas_used::BIGINT,
        hex(extra_data) AS extra_data_hex,
        timestamp::BIGINT,
        base_fee_per_gas::BIGINT,
        chain_id::BIGINT
      FROM v_blocks_new
    """

    cur.execute(
        """
        CREATE TEMP TABLE _stg_blocks (
          block_number BIGINT,
          block_hash_hex TEXT,
          author_hex TEXT,
          gas_used BIGINT,
          extra_data_hex TEXT,
          timestamp BIGINT,
          base_fee_per_gas BIGINT,
          chain_id BIGINT
        )
        """
    )
    copy_query_to_table(
        cur,
        duck,
        query,
        "_stg_blocks",
        columns,
        batch_size=ETL_BATCH_SIZE,
        log_every_batches=ETL_LOG_EVERY_BATCHES,
        label="blocks",
    )
    cur.execute(
        """
        INSERT INTO blocks (block_number, block_hash, author, gas_used, extra_data, timestamp, base_fee_per_gas, chain_id)
        SELECT
          block_number,
          decode(block_hash_hex, 'hex'),
          decode(author_hex, 'hex'),
          gas_used,
          decode(extra_data_hex, 'hex'),
          timestamp,
          base_fee_per_gas,
          chain_id
        FROM _stg_blocks
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


def load_tx(cur: psycopg.Cursor, duck: duckdb.DuckDBPyConnection) -> None:
    columns = [
        "transaction_hash_hex",
        "block_number",
        "transaction_index",
        "nonce",
        "from_address_hex",
        "to_address_hex",
        "value_string",
        "value_f64",
        "input_hex",
        "gas_limit",
        "gas_used",
        "gas_price",
        "transaction_type",
        "max_priority_fee_per_gas",
        "max_fee_per_gas",
        "success",
        "n_input_bytes",
        "n_input_zero_bytes",
        "n_input_nonzero_bytes",
        "chain_id",
        "block_timestamp",
    ]
    query = """
      SELECT
        hex(transaction_hash) AS transaction_hash_hex,
        block_number::BIGINT,
        transaction_index::BIGINT,
        nonce::BIGINT,
        hex(from_address) AS from_address_hex,
        hex(to_address) AS to_address_hex,
        value_string,
        value_f64,
        hex(input) AS input_hex,
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
    """

    cur.execute(
        """
        CREATE TEMP TABLE _stg_tx (
          transaction_hash_hex TEXT,
          block_number BIGINT,
          transaction_index BIGINT,
          nonce BIGINT,
          from_address_hex TEXT,
          to_address_hex TEXT,
          value_string TEXT,
          value_f64 DOUBLE PRECISION,
          input_hex TEXT,
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
        )
        """
    )
    copy_query_to_table(
        cur,
        duck,
        query,
        "_stg_tx",
        columns,
        batch_size=ETL_BATCH_SIZE,
        log_every_batches=ETL_LOG_EVERY_BATCHES,
        label="tx",
    )
    cur.execute(
        """
        INSERT INTO tx (
          transaction_hash, block_number, transaction_index, nonce, from_address, to_address,
          value_string, value_f64, input, gas_limit, gas_used, gas_price, transaction_type,
          max_priority_fee_per_gas, max_fee_per_gas, success, n_input_bytes, n_input_zero_bytes,
          n_input_nonzero_bytes, chain_id, block_timestamp
        )
        SELECT
          decode(transaction_hash_hex, 'hex'),
          block_number,
          transaction_index,
          nonce,
          decode(from_address_hex, 'hex'),
          decode(to_address_hex, 'hex'),
          value_string,
          value_f64,
          decode(input_hex, 'hex'),
          gas_limit,
          gas_used,
          gas_price,
          transaction_type,
          max_priority_fee_per_gas, max_fee_per_gas, success, n_input_bytes, n_input_zero_bytes,
          n_input_nonzero_bytes, chain_id, block_timestamp
        FROM _stg_tx
        ON CONFLICT (transaction_hash) DO NOTHING
        """
    )


def load_address_tx(cur: psycopg.Cursor, duck: duckdb.DuckDBPyConnection) -> None:
    columns = [
        "address_hex",
        "transaction_hash_hex",
        "block_number",
        "transaction_index",
        "direction",
        "counterparty_hex",
        "value_string",
        "value_f64",
        "success",
        "block_timestamp",
        "chain_id",
    ]
    query = """
      SELECT
        hex(address) AS address_hex,
        hex(transaction_hash) AS transaction_hash_hex,
        block_number::BIGINT,
        transaction_index::BIGINT,
        direction,
        hex(counterparty) AS counterparty_hex,
        value_string,
        value_f64,
        success,
        block_timestamp::BIGINT,
        chain_id::BIGINT
      FROM v_address_tx_new
    """

    cur.execute(
        """
        CREATE TEMP TABLE _stg_address_tx (
          address_hex TEXT,
          transaction_hash_hex TEXT,
          block_number BIGINT,
          transaction_index BIGINT,
          direction SMALLINT,
          counterparty_hex TEXT,
          value_string TEXT,
          value_f64 DOUBLE PRECISION,
          success BOOLEAN,
          block_timestamp BIGINT,
          chain_id BIGINT
        )
        """
    )
    copy_query_to_table(
        cur,
        duck,
        query,
        "_stg_address_tx",
        columns,
        batch_size=ETL_BATCH_SIZE,
        log_every_batches=ETL_LOG_EVERY_BATCHES,
        label="address_tx",
    )
    cur.execute(
        """
        INSERT INTO address_tx (
          address, transaction_hash, block_number, transaction_index, direction, counterparty,
          value_string, value_f64, success, block_timestamp, chain_id
        )
        SELECT
          decode(address_hex, 'hex'),
          decode(transaction_hash_hex, 'hex'),
          block_number,
          transaction_index,
          direction,
          decode(counterparty_hex, 'hex'),
          value_string,
          value_f64,
          success,
          block_timestamp,
          chain_id
        FROM _stg_address_tx
        ON CONFLICT (address, block_number, transaction_index, transaction_hash, direction) DO NOTHING
        """
    )


def main() -> None:
    duck = duckdb.connect(database=":memory:")
    duck.execute("PRAGMA threads=4;")

    duck.execute(f"CREATE VIEW v_blocks_all AS SELECT * FROM read_parquet('{BLOCKS_GLOB}');")
    duck.execute(f"CREATE VIEW v_tx_raw_all AS SELECT * FROM read_parquet('{TX_GLOB}');")

    with psycopg.connect(DATABASE_URL) as con:
        with con.cursor() as cur:
            cur.execute(DDL)

            last_ingested_block = get_last_ingested_block(cur, INGEST_PIPELINE)
            max_available_block = int(
                duck.execute("SELECT COALESCE(MAX(block_number), 0)::BIGINT FROM v_blocks_all").fetchone()[0]
            )

            if max_available_block <= last_ingested_block:
                con.commit()
                print(
                    f"No new blocks. last_ingested_block={last_ingested_block}, "
                    f"max_available_block={max_available_block}"
                )
                return

            duck.execute(
                f"CREATE VIEW v_blocks_new AS SELECT * FROM v_blocks_all WHERE block_number > {last_ingested_block};"
            )
            duck.execute(
                f"CREATE VIEW v_tx_raw_new AS SELECT * FROM v_tx_raw_all WHERE block_number > {last_ingested_block};"
            )
            duck.execute(
                """
                CREATE VIEW v_tx_new AS
                SELECT
                  t.transaction_hash,
                  t.block_number,
                  t.transaction_index,
                  t.nonce,
                  t.from_address,
                  NULLIF(t.to_address, ''::BLOB) AS to_address,
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
            duck.execute(
                """
                CREATE VIEW v_address_tx_new AS
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
                FROM v_tx_new
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
                FROM v_tx_new
                WHERE to_address IS NOT NULL
                """
            )

            new_blocks = int(duck.execute("SELECT COUNT(*) FROM v_blocks_new").fetchone()[0])
            new_tx = int(duck.execute("SELECT COUNT(*) FROM v_tx_new").fetchone()[0])
            print(
                f"Loading new data: blocks={new_blocks}, tx={new_tx}, "
                f"from_block>{last_ingested_block}..{max_available_block}"
            )

            load_blocks(cur, duck)
            load_tx(cur, duck)
            load_address_tx(cur, duck)
            set_last_ingested_block(cur, INGEST_PIPELINE, max_available_block)
            con.commit()

    print("Serving tables updated incrementally: blocks, tx, address_tx")


if __name__ == "__main__":
    main()
