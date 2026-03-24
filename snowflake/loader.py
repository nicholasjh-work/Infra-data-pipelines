"""Loader script for uploading CSV data into Snowflake.

Reads CSV files produced by the synthetic data generator and writes them
into Snowflake tables in the RAW schema using write_pandas for bulk load.

Connection parameters are read from environment variables. See .env.example.

Usage:
    python snowflake/loader.py --data-dir ./data
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


def get_connection() -> snowflake.connector.SnowflakeConnection:
    """Instantiate a Snowflake connection using environment variables.

    Raises RuntimeError if required variables are missing.
    """
    required = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_DATABASE",
        "SNOWFLAKE_WAREHOUSE",
    ]
    missing = [v for v in required if not os.getenv(v)]
    if missing:
        raise RuntimeError(
            f"Missing required environment variables: {', '.join(missing)}"
        )
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        role=os.getenv("SNOWFLAKE_ROLE"),
    )


def load_csv_to_table(
    conn: snowflake.connector.SnowflakeConnection,
    csv_path: Path,
    schema: str = "RAW",
) -> None:
    """Load a single CSV into the corresponding Snowflake table.

    Table name is derived from the filename (e.g. members.csv -> RAW.MEMBERS).
    """
    table_name = csv_path.stem.upper()
    df = pd.read_csv(csv_path)
    cs = conn.cursor()
    try:
        # Parameterized check for table existence
        cs.execute(
            "SELECT 1 FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
            (schema, table_name),
        )
        if not cs.fetchone():
            raise RuntimeError(
                f"Target table {schema}.{table_name} does not exist. Run ddl.sql first."
            )
        success, _, _ = write_pandas(
            conn,
            df,
            table_name=table_name,
            database=conn.database,
            schema=schema,
            overwrite=True,
        )
        if not success:
            raise RuntimeError(f"Failed to load data into {schema}.{table_name}")
        print(f"Loaded {len(df):,} rows into {schema}.{table_name}")
    finally:
        cs.close()


def load_all(data_dir: Path, schema: str = "RAW") -> None:
    """Load all CSV files in the directory into Snowflake."""
    conn = get_connection()
    try:
        for csv_file in sorted(data_dir.glob("*.csv")):
            load_csv_to_table(conn, csv_file, schema=schema)
    finally:
        conn.close()


def main(args: list[str]) -> None:
    parser = argparse.ArgumentParser(description="Load CSV data into Snowflake")
    parser.add_argument("--data-dir", required=True, type=str, help="Directory with CSV files")
    parser.add_argument("--schema", default="RAW", type=str, help="Target schema (default: RAW)")
    parsed = parser.parse_args(args)
    data_dir = Path(parsed.data_dir)
    if not data_dir.is_dir():
        raise FileNotFoundError(f"{data_dir} does not exist or is not a directory")
    load_all(data_dir, schema=parsed.schema)


if __name__ == "__main__":  # pragma: no cover
    main(sys.argv[1:])
