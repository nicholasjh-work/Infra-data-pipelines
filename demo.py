"""Run the full infra demo: generate data, load into DuckDB, print stats.

Usage:
    python demo.py
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import duckdb


def run_cmd(cmd: list[str], label: str) -> None:
    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"{'='*60}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        sys.exit(1)


def print_stats(db_path: str = "demo.duckdb") -> None:
    con = duckdb.connect(db_path, read_only=True)

    print(f"\n{'='*60}")
    print("  DATA SUMMARY")
    print(f"{'='*60}")

    tables = ["members", "daily_metrics", "feature_events", "sessions",
              "experiments", "experiment_assignments", "subscriptions"]
    for t in tables:
        count = con.execute(f"SELECT COUNT(*) FROM raw.{t}").fetchone()[0]
        print(f"  raw.{t}: {count:,} rows")

    print(f"\n--- Members by Plan Type ---")
    rows = con.execute("""
        SELECT plan_type, COUNT(*) as n,
               ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as pct
        FROM raw.members GROUP BY 1 ORDER BY 2 DESC
    """).fetchall()
    for r in rows:
        print(f"  {r[0]:12s}  {r[1]:>6,}  ({r[2]}%)")

    print(f"\n--- Members by Acquisition Channel ---")
    rows = con.execute("""
        SELECT acquisition_channel, COUNT(*) as n
        FROM raw.members GROUP BY 1 ORDER BY 2 DESC
    """).fetchall()
    for r in rows:
        print(f"  {r[0]:12s}  {r[1]:>6,}")

    print(f"\n--- Daily Metrics Sample (first 5 rows) ---")
    rows = con.execute("""
        SELECT member_id, metric_date, ROUND(hrv, 1) as hrv,
               ROUND(strain, 1) as strain, ROUND(recovery, 1) as recovery,
               ROUND(sleep_hours, 1) as sleep_hrs
        FROM raw.daily_metrics ORDER BY member_id, metric_date LIMIT 5
    """).fetchall()
    print(f"  {'member':>8s}  {'date':>12s}  {'hrv':>6s}  {'strain':>6s}  {'recov':>6s}  {'sleep':>6s}")
    for r in rows:
        print(f"  {r[0]:>8}  {str(r[1]):>12s}  {r[2]:>6.1f}  {r[3]:>6.1f}  {r[4]:>6.1f}  {r[5]:>6.1f}")

    print(f"\n--- Top 10 Feature Events ---")
    rows = con.execute("""
        SELECT event_name, COUNT(*) as n
        FROM raw.feature_events GROUP BY 1 ORDER BY 2 DESC LIMIT 10
    """).fetchall()
    for r in rows:
        print(f"  {r[0]:35s}  {r[1]:>6,}")

    print(f"\n--- Churn Analysis ---")
    row = con.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN end_date IS NOT NULL THEN 1 ELSE 0 END) as churned,
            ROUND(SUM(CASE WHEN end_date IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as churn_pct
        FROM raw.subscriptions
    """).fetchone()
    print(f"  Total members: {row[0]:,}")
    print(f"  Churned:       {row[1]:,} ({row[2]}%)")
    print(f"  Active:        {row[0] - row[1]:,} ({100 - row[2]}%)")

    print(f"\n--- Experiments ---")
    rows = con.execute("SELECT * FROM raw.experiments").fetchall()
    for r in rows:
        print(f"  Experiment {r[0]}: {r[1]} ({r[2]} to {r[3]})")

    con.close()
    print(f"\n{'='*60}")
    print("  DEMO COMPLETE - Database: demo.duckdb")
    print(f"{'='*60}")


def main() -> None:
    root = Path(__file__).parent

    # Step 1: Generate data
    run_cmd(
        [sys.executable, str(root / "data_generator" / "generate.py"),
         "--output-dir", str(root / "data"), "--seed", "42"],
        "STEP 1: Generating synthetic data"
    )

    # Step 2: Load into DuckDB
    run_cmd(
        [sys.executable, str(root / "duckdb_loader.py"),
         "--data-dir", str(root / "data"), "--db", str(root / "demo.duckdb")],
        "STEP 2: Loading into DuckDB"
    )

    # Step 3: Print stats
    print_stats(str(root / "demo.duckdb"))


if __name__ == "__main__":
    main()
