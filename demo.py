"""Run the full infra demo: generate data, load into PostgreSQL, print stats.

Usage:
    python demo.py
    python demo.py --db-url postgresql://user:pass@host/dbname
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

from sqlalchemy import create_engine, text

DEFAULT_DB_URL = "postgresql://demo_user:demo_pass@localhost:5432/analytics_demo"


def run_cmd(cmd, label):
    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"{'='*60}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        sys.exit(1)


def print_stats(db_url):
    engine = create_engine(db_url)
    print(f"\n{'='*60}")
    print("  DATA SUMMARY")
    print(f"{'='*60}")
    with engine.connect() as conn:
        for t in ["members","daily_metrics","feature_events","sessions","experiments","experiment_assignments","subscriptions"]:
            count = conn.execute(text(f"SELECT COUNT(*) FROM raw.{t}")).scalar()
            print(f"  raw.{t}: {count:,} rows")

        print(f"\n--- Members by Plan Type ---")
        for r in conn.execute(text("SELECT plan_type, COUNT(*) n, ROUND(COUNT(*)*100.0/SUM(COUNT(*)) OVER(),1) pct FROM raw.members GROUP BY 1 ORDER BY 2 DESC")).fetchall():
            print(f"  {r[0]:12s}  {r[1]:>6,}  ({r[2]}%)")

        print(f"\n--- Churn Analysis ---")
        row = conn.execute(text("SELECT COUNT(*), SUM(CASE WHEN end_date IS NOT NULL THEN 1 ELSE 0 END), ROUND(SUM(CASE WHEN end_date IS NOT NULL THEN 1 ELSE 0 END)*100.0/COUNT(*),1) FROM raw.subscriptions")).fetchone()
        print(f"  Total: {row[0]:,}  Churned: {row[1]:,} ({row[2]}%)  Active: {row[0]-row[1]:,}")

        print(f"\n--- Top 10 Feature Events ---")
        for r in conn.execute(text("SELECT event_name, COUNT(*) n FROM raw.feature_events GROUP BY 1 ORDER BY 2 DESC LIMIT 10")).fetchall():
            print(f"  {r[0]:35s}  {r[1]:>6,}")

        print(f"\n--- Experiments ---")
        for r in conn.execute(text("SELECT * FROM raw.experiments ORDER BY experiment_id")).fetchall():
            print(f"  Experiment {r[0]}: {r[1]} ({r[2]} to {r[3]})")

    engine.dispose()
    print(f"\n{'='*60}")
    print("  DEMO COMPLETE")
    print(f"{'='*60}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-url", default=os.getenv("DATABASE_URL", DEFAULT_DB_URL))
    args = parser.parse_args()
    root = Path(__file__).parent
    run_cmd([sys.executable, str(root/"data_generator"/"generate.py"), "--output-dir", str(root/"data"), "--seed", "42"], "STEP 1: Generating synthetic data")
    run_cmd([sys.executable, str(root/"pg_loader.py"), "--data-dir", str(root/"data"), "--db-url", args.db_url], "STEP 2: Loading into PostgreSQL")
    print_stats(args.db_url)


if __name__ == "__main__":
    main()
