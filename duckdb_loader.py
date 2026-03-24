"""Load generated CSVs into a local DuckDB database.

This is the local demo path. For production, use snowflake/loader.py.

Usage:
    python duckdb_loader.py --data-dir data/
"""
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


def load_all(data_dir: Path, db_path: Path) -> None:
    """Load all CSVs into DuckDB with schema matching Snowflake DDL."""
    con = duckdb.connect(str(db_path))

    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute("CREATE SCHEMA IF NOT EXISTS analytics")

    table_map = {
        "members": """
            CREATE OR REPLACE TABLE raw.members AS
            SELECT * FROM read_csv_auto(?)
        """,
        "daily_metrics": """
            CREATE OR REPLACE TABLE raw.daily_metrics AS
            SELECT * FROM read_csv_auto(?)
        """,
        "feature_events": """
            CREATE OR REPLACE TABLE raw.feature_events AS
            SELECT * FROM read_csv_auto(?)
        """,
        "sessions": """
            CREATE OR REPLACE TABLE raw.sessions AS
            SELECT * FROM read_csv_auto(?)
        """,
        "experiments": """
            CREATE OR REPLACE TABLE raw.experiments AS
            SELECT * FROM read_csv_auto(?)
        """,
        "experiment_assignments": """
            CREATE OR REPLACE TABLE raw.experiment_assignments AS
            SELECT * FROM read_csv_auto(?)
        """,
        "subscriptions": """
            CREATE OR REPLACE TABLE raw.subscriptions AS
            SELECT * FROM read_csv_auto(?)
        """,
    }

    for name, sql in table_map.items():
        csv_path = data_dir / f"{name}.csv"
        if not csv_path.exists():
            print(f"  SKIP {name}.csv (not found)")
            continue
        con.execute(sql, [str(csv_path)])
        count = con.execute(f"SELECT COUNT(*) FROM raw.{name}").fetchone()[0]
        print(f"  raw.{name}: {count:,} rows")

    con.close()
    print(f"\nDatabase written to {db_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Load CSVs into DuckDB")
    parser.add_argument("--data-dir", type=str, default="data", help="CSV directory")
    parser.add_argument("--db", type=str, default="demo.duckdb", help="Output DuckDB file")
    args = parser.parse_args()
    data_dir = Path(args.data_dir)
    if not data_dir.is_dir():
        raise FileNotFoundError(f"{data_dir} not found. Run generate.py first.")
    print("Loading CSVs into DuckDB...")
    load_all(data_dir, Path(args.db))


if __name__ == "__main__":
    main()
