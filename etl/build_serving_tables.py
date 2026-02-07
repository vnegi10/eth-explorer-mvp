import os
import tempfile

import duckdb
import psycopg
from dotenv import load_dotenv
from psycopg import sql

load_dotenv()

PARQUET_DIR = os.environ["PARQUET_DIR"]
DATABASE_URL = os.environ["DATABASE_URL"]
INGEST_PIPELINE = "serving_tables_v1"

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


def export_query_to_csv(duck: duckdb.DuckDBPyConnection, query: str, path: str) -> None:
    duck.execute(f"COPY ({query}) TO '{path}' (HEADER, DELIMITER ',');")


def copy_csv_to_table(cur: psycopg.Cursor, table_name: str, columns: list[str], csv_path: str) -> None:
    with cur.copy(
        sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT csv, HEADER true)").format(
            sql.Identifier(table_name),
            sql.SQL(", ").join(map(sql.Identifier, columns)),
        )
    ) as copy:
        with open(csv_path, "rb") as handle:
            while chunk := handle.read(1024 * 1024):
                copy.write(chunk)


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
        "block_hash",
        "author",
        "gas_used",
        "extra_data",
        "timestamp",
        "base_fee_per_gas",
        "chain_id",
    ]
    query = """
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
    """

    with tempfile.NamedTemporaryFile(suffix=".csv", delete=True) as tmp:
        export_query_to_csv(duck, query, tmp.name)
        cur.execute("CREATE TEMP TABLE _stg_blocks (LIKE blocks)")
        copy_csv_to_table(cur, "_stg_blocks", columns, tmp.name)
        cur.execute(
            """
            INSERT INTO blocks (block_number, block_hash, author, gas_used, extra_data, timestamp, base_fee_per_gas, chain_id)
            SELECT block_number, block_hash, author, gas_used, extra_data, timestamp, base_fee_per_gas, chain_id
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
        "transaction_hash",
        "block_number",
        "transaction_index",
        "nonce",
        "from_address",
        "to_address",
        "value_string",
        "value_f64",
        "input",
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
        transaction_hash,
        block_number::BIGINT,
        transaction_index::BIGINT,
        nonce::BIGINT,
        from_address,
        to_address,
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
    """

    with tempfile.NamedTemporaryFile(suffix=".csv", delete=True) as tmp:
        export_query_to_csv(duck, query, tmp.name)
        cur.execute("CREATE TEMP TABLE _stg_tx (LIKE tx)")
        copy_csv_to_table(cur, "_stg_tx", columns, tmp.name)
        cur.execute(
            """
            INSERT INTO tx (
              transaction_hash, block_number, transaction_index, nonce, from_address, to_address,
              value_string, value_f64, input, gas_limit, gas_used, gas_price, transaction_type,
              max_priority_fee_per_gas, max_fee_per_gas, success, n_input_bytes, n_input_zero_bytes,
              n_input_nonzero_bytes, chain_id, block_timestamp
            )
            SELECT
              transaction_hash, block_number, transaction_index, nonce, from_address, to_address,
              value_string, value_f64, input, gas_limit, gas_used, gas_price, transaction_type,
              max_priority_fee_per_gas, max_fee_per_gas, success, n_input_bytes, n_input_zero_bytes,
              n_input_nonzero_bytes, chain_id, block_timestamp
            FROM _stg_tx
            ON CONFLICT (transaction_hash) DO NOTHING
            """
        )


def load_address_tx(cur: psycopg.Cursor, duck: duckdb.DuckDBPyConnection) -> None:
    columns = [
        "address",
        "transaction_hash",
        "block_number",
        "transaction_index",
        "direction",
        "counterparty",
        "value_string",
        "value_f64",
        "success",
        "block_timestamp",
        "chain_id",
    ]
    query = """
      SELECT
        address,
        transaction_hash,
        block_number::BIGINT,
        transaction_index::BIGINT,
        direction,
        counterparty,
        value_string,
        value_f64,
        success,
        block_timestamp::BIGINT,
        chain_id::BIGINT
      FROM v_address_tx_new
    """

    with tempfile.NamedTemporaryFile(suffix=".csv", delete=True) as tmp:
        export_query_to_csv(duck, query, tmp.name)
        cur.execute("CREATE TEMP TABLE _stg_address_tx (LIKE address_tx)")
        copy_csv_to_table(cur, "_stg_address_tx", columns, tmp.name)
        cur.execute(
            """
            INSERT INTO address_tx (
              address, transaction_hash, block_number, transaction_index, direction, counterparty,
              value_string, value_f64, success, block_timestamp, chain_id
            )
            SELECT
              address, transaction_hash, block_number, transaction_index, direction, counterparty,
              value_string, value_f64, success, block_timestamp, chain_id
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
