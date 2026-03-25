"""Load generated CSVs into a local PostgreSQL database.

This is the local demo path. For production, use snowflake/loader.py.

Requires a PostgreSQL database. Default connection uses environment
variables or falls back to local defaults.

Usage:
    python pg_loader.py --data-dir data/
    python pg_loader.py --data-dir data/ --db-url postgresql://user:pass@host/dbname
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

DEFAULT_DB_URL = "postgresql://demo_user:demo_pass@localhost:5432/analytics_demo"

DDL = """
CREATE SCHEMA IF NOT EXISTS raw;

DROP TABLE IF EXISTS raw.experiment_assignments CASCADE;
DROP TABLE IF EXISTS raw.experiments CASCADE;
DROP TABLE IF EXISTS raw.subscriptions CASCADE;
DROP TABLE IF EXISTS raw.sessions CASCADE;
DROP TABLE IF EXISTS raw.feature_events CASCADE;
DROP TABLE IF EXISTS raw.daily_metrics CASCADE;
DROP TABLE IF EXISTS raw.members CASCADE;

CREATE TABLE raw.members (
    member_id         INTEGER PRIMARY KEY,
    signup_date       DATE,
    plan_type         VARCHAR(20),
    age_group         VARCHAR(10),
    gender            VARCHAR(20),
    acquisition_channel VARCHAR(20),
    region            VARCHAR(30)
);

CREATE TABLE raw.daily_metrics (
    member_id         INTEGER,
    metric_date       DATE,
    hrv               FLOAT,
    resting_heart_rate FLOAT,
    sleep_hours       FLOAT,
    sleep_quality     FLOAT,
    strain            FLOAT,
    recovery          FLOAT,
    calories          FLOAT,
    PRIMARY KEY (member_id, metric_date)
);

CREATE TABLE raw.feature_events (
    id                SERIAL PRIMARY KEY,
    member_id         INTEGER,
    event_date        DATE,
    feature           VARCHAR(50),
    event_name        VARCHAR(50)
);

CREATE TABLE raw.sessions (
    id                SERIAL PRIMARY KEY,
    member_id         INTEGER,
    session_start     TIMESTAMP,
    session_end       TIMESTAMP,
    device_type       VARCHAR(20),
    os_version        VARCHAR(20),
    location          VARCHAR(30)
);

CREATE TABLE raw.experiments (
    experiment_id     INTEGER PRIMARY KEY,
    experiment_name   VARCHAR(100),
    start_date        DATE,
    end_date          DATE,
    description       TEXT
);

CREATE TABLE raw.experiment_assignments (
    id                SERIAL PRIMARY KEY,
    member_id         INTEGER,
    experiment_id     INTEGER,
    variant           VARCHAR(20),
    assigned_date     DATE
);

CREATE TABLE raw.subscriptions (
    member_id         INTEGER PRIMARY KEY,
    plan_type         VARCHAR(20),
    start_date        DATE,
    end_date          DATE,
    auto_renew        BOOLEAN
);

CREATE INDEX idx_daily_metrics_date ON raw.daily_metrics(metric_date);
CREATE INDEX idx_daily_metrics_member ON raw.daily_metrics(member_id);
CREATE INDEX idx_feature_events_date ON raw.feature_events(event_date);
CREATE INDEX idx_feature_events_member ON raw.feature_events(member_id);
CREATE INDEX idx_sessions_start ON raw.sessions(session_start);
CREATE INDEX idx_exp_assign_exp ON raw.experiment_assignments(experiment_id);
"""

TABLE_ORDER = [
    "members",
    "daily_metrics",
    "feature_events",
    "sessions",
    "experiments",
    "experiment_assignments",
    "subscriptions",
]


def load_all(data_dir: Path, db_url: str) -> None:
    """Create schema, tables, and load all CSVs."""
    engine = create_engine(db_url)

    print("  Creating schema and tables...")
    with engine.begin() as conn:
        for statement in DDL.split(";"):
            stmt = statement.strip()
            if stmt:
                conn.execute(text(stmt))

    for name in TABLE_ORDER:
        csv_path = data_dir / f"{name}.csv"
        if not csv_path.exists():
            print(f"  SKIP {name}.csv (not found)")
            continue
        # Use chunked loading for large files
        total = 0
        for chunk in pd.read_csv(csv_path, chunksize=50000):
            chunk.columns = [c.lower() for c in chunk.columns]
            chunk.to_sql(
                name,
                engine,
                schema="raw",
                if_exists="append",
                index=False,
                method="multi",
            )
            total += len(chunk)
        print(f"  raw.{name}: {total:,} rows")

    engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser(description="Load CSVs into PostgreSQL")
    parser.add_argument("--data-dir", type=str, default="data", help="CSV directory")
    parser.add_argument(
        "--db-url",
        type=str,
        default=os.getenv("DATABASE_URL", DEFAULT_DB_URL),
        help="PostgreSQL connection URL",
    )
    args = parser.parse_args()
    data_dir = Path(args.data_dir)
    if not data_dir.is_dir():
        raise FileNotFoundError(f"{data_dir} not found. Run generate.py first.")
    print("Loading CSVs into PostgreSQL...")
    load_all(data_dir, args.db_url)
    print("Done.")


if __name__ == "__main__":
    main()
