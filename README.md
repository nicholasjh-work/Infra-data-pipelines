# Shared Infrastructure: Data Generation & Ingestion

**[Live Interactive Dashboard](https://nicholasjh-work.github.io/feature-adoption-retention/)**

Foundational data layer for a wearable health analytics platform. Generates 12 months of synthetic member data, loads it into PostgreSQL (local demo) or Snowflake (production), and provides the raw tables consumed by downstream dbt projects.

## Quick Start

```bash
git clone https://github.com/nicholasjh-work/Infra-data-pipelines.git
cd Infra-data-pipelines
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Set up PostgreSQL (one time)
createdb analytics_demo
psql analytics_demo -c "CREATE USER demo_user WITH PASSWORD 'demo_pass';"
psql analytics_demo -c "GRANT ALL PRIVILEGES ON DATABASE analytics_demo TO demo_user;"
psql analytics_demo -c "GRANT ALL ON SCHEMA public TO demo_user;"

# Run the full demo
python demo.py
```

## Architecture

```
config.yaml
    ↓
generate.py → 7 CSV tables (1.4M+ rows, ~15 seconds)
    ↓
pg_loader.py → PostgreSQL raw schema (local demo)
snowflake/loader.py → Snowflake RAW schema (production)
```

## Tables Generated

| Table | Rows | Description |
|-------|------|-------------|
| `members` | 10,000 | Demographics, plan type, acquisition channel, region |
| `daily_metrics` | ~1.1M | HRV, strain, recovery, sleep hours/quality, calories |
| `feature_events` | ~160K | Event-level feature interactions (15 event types, 8 features) |
| `sessions` | ~111K | App sessions with device type, duration, pages viewed |
| `experiments` | 2 | A/B test definitions with hypothesis and date range |
| `experiment_assignments` | 20,000 | Member-variant assignments (50/50 control/treatment) |
| `subscriptions` | 10,000 | Plan type, start/end dates, auto-renew status |

## Demo Output

```
--- Members by Plan Type ---
  free           5,042  (50.4%)
  pro            3,978  (39.8%)
  enterprise       980  (9.8%)

--- Churn Analysis ---
  Total: 10,000  Churned: 5,815 (58.2%)  Active: 4,185

--- Experiments ---
  Experiment 1: Reminder Timing (2025-06-22 to 2025-07-22)
  Experiment 2: New Coaching Tip UI (2025-09-20 to 2025-10-20)
```

## Snowflake (Production Path)

```bash
cp .env.example .env    # Edit with your Snowflake credentials
# Run snowflake/ddl.sql in your Snowflake worksheet
python snowflake/loader.py --data-dir data/
```

## Related Repos

- [feature-adoption-retention](https://github.com/nicholasjh-work/feature-adoption-retention) - dbt models for weekly adoption metrics and cohort retention curves
- [experimentation-segmentation](https://github.com/nicholasjh-work/Experimentation-segmentation) - A/B test analysis and K-means user segmentation

## Tech Stack

Python, NumPy, pandas, Faker, PostgreSQL, Snowflake, dbt
