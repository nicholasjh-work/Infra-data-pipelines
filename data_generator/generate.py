"""Synthetic data generator for a wearable health platform.

Produces CSV files emulating a production analytics pipeline: demographics,
physiological measurements, feature usage, sessions, experiments, and
subscriptions.  Uses vectorized NumPy/pandas operations throughout to
generate 10K members x 365 days in under 60 seconds.

Configuration lives in config.yaml.
"""

from __future__ import annotations

import argparse
import datetime as dt
import pathlib
from dataclasses import dataclass
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import yaml
from faker import Faker


@dataclass
class Config:
    """Configuration parameters for synthetic data generation."""

    member_count: int
    start_date: dt.date
    end_date: dt.date
    churn_rates: Dict[int, float]
    seasonal_factors: Dict[int, float]
    experiments: List[Dict[str, object]]


def load_config(path: pathlib.Path) -> Config:
    """Load configuration from YAML. Defaults to last 365 days if dates omitted."""
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    member_count = int(raw.get("member_count", 10000))
    today = dt.date.today()
    start_str = raw.get("start_date")
    end_str = raw.get("end_date")
    if start_str and end_str:
        start_date = dt.datetime.strptime(start_str, "%Y-%m-%d").date()
        end_date = dt.datetime.strptime(end_str, "%Y-%m-%d").date()
    else:
        end_date = today
        start_date = end_date - dt.timedelta(days=365)
    churn_rates = {int(k): float(v) for k, v in raw.get("churn_rates", {}).items()}
    seasonal_factors = {
        int(k): float(v) for k, v in raw.get("seasonal_factors", {}).items()
    }
    experiments = raw.get("experiments", [])
    return Config(
        member_count=member_count,
        start_date=start_date,
        end_date=end_date,
        churn_rates=churn_rates,
        seasonal_factors=seasonal_factors,
        experiments=experiments,
    )


def _sample_churn_months(churn_rates: Dict[int, float], n: int) -> np.ndarray:
    """Vectorized churn month sampling. Returns array of ints; -1 means no churn."""
    months = sorted(churn_rates.keys())
    probs = [churn_rates[m] for m in months]
    remaining = max(0.0, 1.0 - sum(probs))
    choices = months + [-1]
    probs_full = np.array(probs + [remaining])
    probs_full /= probs_full.sum()  # normalize to handle float precision
    return np.random.choice(choices, size=n, p=probs_full)


def generate_members(cfg: Config) -> pd.DataFrame:
    """Generate the members table."""
    n = cfg.member_count
    days_range = (cfg.end_date - cfg.start_date).days
    signup_offsets = np.random.randint(0, days_range, size=n)
    signup_dates = pd.to_datetime(cfg.start_date) + pd.to_timedelta(
        signup_offsets, unit="D"
    )

    df = pd.DataFrame(
        {
            "member_id": np.arange(1, n + 1),
            "signup_date": signup_dates.date,
            "plan_type": np.random.choice(
                ["free", "pro", "enterprise"], size=n, p=[0.5, 0.4, 0.1]
            ),
            "age_group": np.random.choice(
                ["18-25", "26-35", "36-45", "46-55", "56+"],
                size=n,
                p=[0.2, 0.3, 0.25, 0.15, 0.10],
            ),
            "gender": np.random.choice(
                ["female", "male", "other", "prefer_not_to_say"],
                size=n,
                p=[0.48, 0.48, 0.02, 0.02],
            ),
            "acquisition_channel": np.random.choice(
                ["organic", "social", "paid", "referral"],
                size=n,
                p=[0.4, 0.3, 0.2, 0.1],
            ),
            "region": np.random.choice(
                ["North America", "Europe", "Asia", "South America", "Other"],
                size=n,
                p=[0.4, 0.25, 0.2, 0.1, 0.05],
            ),
        }
    )
    return df


def assign_churn_dates(members: pd.DataFrame, cfg: Config) -> pd.Series:
    """Assign churn date (or NaT) per member based on churn rates."""
    churn_months = _sample_churn_months(cfg.churn_rates, len(members))
    signup_dates = pd.to_datetime(members["signup_date"])
    # churn_month * 30 days after signup
    offsets = pd.to_timedelta((churn_months + 1) * 30, unit="D")
    churn_dates = signup_dates + offsets
    # -1 means no churn
    no_churn_mask = churn_months == -1
    churn_dates[no_churn_mask] = pd.NaT
    # clamp: if churn date is after end_date, treat as no churn
    churn_dates[churn_dates > pd.Timestamp(cfg.end_date)] = pd.NaT
    return churn_dates


def generate_daily_metrics(
    members: pd.DataFrame, churn_dates: pd.Series, cfg: Config
) -> pd.DataFrame:
    """Generate daily_metrics using vectorized date expansion.

    Column names match the Snowflake DDL: HRV, RESTING_HEART_RATE,
    SLEEP_HOURS, SLEEP_QUALITY, STRAIN, RECOVERY, CALORIES.
    """
    signup_dates = pd.to_datetime(members["signup_date"])
    end_dates = churn_dates.fillna(pd.Timestamp(cfg.end_date))
    # Build member-day pairs using repeat + date_range
    member_ids = []
    dates = []
    for _, row in members.iterrows():
        mid = row["member_id"]
        s = pd.Timestamp(row["signup_date"])
        e = end_dates.iloc[row.name]
        if s > e:
            continue
        day_range = pd.date_range(s, e, freq="D")
        member_ids.append(np.full(len(day_range), mid, dtype=int))
        dates.append(day_range)

    all_member_ids = np.concatenate(member_ids)
    all_dates = np.concatenate(dates)
    n = len(all_member_ids)

    # Seasonal factor lookup
    months = pd.DatetimeIndex(all_dates).month
    factors = np.array([cfg.seasonal_factors.get(m, 1.0) for m in months])

    df = pd.DataFrame(
        {
            "member_id": all_member_ids,
            "metric_date": all_dates,
            "hrv": np.round(np.random.normal(65, 10, n) * factors, 2),
            "resting_heart_rate": np.round(
                np.random.normal(60, 8, n) * (2 - factors), 2
            ),
            "sleep_hours": np.round(
                np.clip(np.random.normal(7, 1, n) * (1 + (factors - 1) / 2), 4, 10), 2
            ),
            "sleep_quality": np.round(
                np.clip(np.random.normal(80, 10, n) * factors, 0, 100), 2
            ),
            "strain": np.round(np.clip(np.random.normal(12, 3, n) * factors, 0, 21), 2),
            "recovery": np.round(
                np.clip(np.random.normal(70, 15, n) * factors, 0, 100), 2
            ),
            "calories": np.round(np.random.normal(2000, 300, n) * factors, 2),
        }
    )
    return df


EVENT_NAMES = [
    "onboarding_completed",
    "first_sleep_tracked",
    "first_workout_logged",
    "health_report_viewed",
    "coaching_tip_opened",
    "coaching_tip_acted_on",
    "heart_rate_alert_received",
    "heart_rate_alert_dismissed",
    "health_goal_set",
    "health_goal_achieved",
    "share_to_social",
    "invite_sent",
    "settings_changed",
    "subscription_upgraded",
    "subscription_downgraded",
]

FEATURE_NAMES = [
    "sleep_tracking",
    "workout_logging",
    "coaching",
    "heart_rate_alert",
    "health_goal",
    "social",
    "account_settings",
    "subscription_management",
]

PLATFORMS = ["ios", "android", "web"]


def generate_feature_events(
    members: pd.DataFrame, churn_dates: pd.Series, cfg: Config
) -> pd.DataFrame:
    """Generate feature_events. Columns match DDL: EVENT_DATE, FEATURE, EVENT_NAME."""
    end_dates = churn_dates.fillna(pd.Timestamp(cfg.end_date))
    signup_dates = pd.to_datetime(members["signup_date"])
    days_active = (end_dates - signup_dates).dt.days.clip(lower=1).values

    # Poisson-distributed event counts per member
    n_events = np.random.poisson(lam=np.maximum(1, days_active / 7))
    total_events = int(n_events.sum())

    # Repeat member info for each event
    member_ids = np.repeat(members["member_id"].values, n_events)
    signups_rep = np.repeat(signup_dates.values, n_events)
    days_rep = np.repeat(days_active, n_events)

    offsets = (np.random.random(total_events) * days_rep).astype(int)
    event_dates = pd.to_datetime(signups_rep) + pd.to_timedelta(offsets, unit="D")

    df = pd.DataFrame(
        {
            "member_id": member_ids,
            "event_date": event_dates.date,
            "feature": np.random.choice(FEATURE_NAMES, size=total_events),
            "event_name": np.random.choice(EVENT_NAMES, size=total_events),
        }
    )
    return df


def generate_sessions(
    members: pd.DataFrame, churn_dates: pd.Series, cfg: Config
) -> pd.DataFrame:
    """Generate sessions. Columns match DDL: SESSION_START, SESSION_END, DEVICE_TYPE, OS_VERSION, LOCATION."""
    end_dates = churn_dates.fillna(pd.Timestamp(cfg.end_date))
    signup_dates = pd.to_datetime(members["signup_date"])
    days_active = (end_dates - signup_dates).dt.days.clip(lower=1).values

    n_sessions = np.random.poisson(lam=np.maximum(1, days_active / 10))
    total = int(n_sessions.sum())

    member_ids = np.repeat(members["member_id"].values, n_sessions)
    signups_rep = np.repeat(signup_dates.values, n_sessions)
    days_rep = np.repeat(days_active, n_sessions)

    day_offsets = (np.random.random(total) * days_rep).astype(int)
    sec_offsets = np.random.randint(0, 86400, size=total)
    starts = (
        pd.to_datetime(signups_rep)
        + pd.to_timedelta(day_offsets, unit="D")
        + pd.to_timedelta(sec_offsets, unit="s")
    )
    durations = np.clip(np.random.normal(20, 10, total).astype(int), 1, 120)
    ends = starts + pd.to_timedelta(durations, unit="m")

    devices = np.random.choice(["ios", "android", "web"], size=total, p=[0.6, 0.3, 0.1])
    os_versions = np.where(
        devices == "ios",
        np.random.choice(["iOS 17.4", "iOS 17.5", "iOS 18.0"], size=total),
        np.where(
            devices == "android",
            np.random.choice(["Android 14", "Android 15"], size=total),
            "Web",
        ),
    )
    locations = np.random.choice(
        ["US-East", "US-West", "US-Central", "EU-West", "EU-Central", "APAC"],
        size=total,
        p=[0.25, 0.20, 0.10, 0.15, 0.15, 0.15],
    )

    df = pd.DataFrame(
        {
            "member_id": member_ids,
            "session_start": starts,
            "session_end": ends,
            "device_type": devices,
            "os_version": os_versions,
            "location": locations,
        }
    )
    return df


def generate_experiments(cfg: Config) -> pd.DataFrame:
    """Create experiments table. Columns match DDL: EXPERIMENT_ID, EXPERIMENT_NAME, START_DATE, END_DATE, DESCRIPTION."""
    records = []
    for exp in cfg.experiments:
        start = cfg.start_date + dt.timedelta(days=int(exp.get("start_offset_days", 0)))
        end = start + dt.timedelta(days=int(exp.get("duration_days", 30)))
        records.append(
            {
                "experiment_id": exp["experiment_id"],
                "experiment_name": exp["experiment_name"],
                "start_date": start,
                "end_date": end,
                "description": exp.get("hypothesis", ""),
            }
        )
    return pd.DataFrame(records)


def generate_experiment_assignments(
    members: pd.DataFrame, experiments: pd.DataFrame
) -> pd.DataFrame:
    """Assign each member to control or treatment for each experiment."""
    records = []
    for _, exp in experiments.iterrows():
        n = len(members)
        records.append(
            pd.DataFrame(
                {
                    "member_id": members["member_id"].values,
                    "experiment_id": exp["experiment_id"],
                    "variant": np.random.choice(["control", "treatment"], size=n),
                    "assigned_date": exp["start_date"],
                }
            )
        )
    return pd.concat(records, ignore_index=True)


def generate_subscriptions(
    members: pd.DataFrame, churn_dates: pd.Series
) -> pd.DataFrame:
    """Create subscriptions. Columns match DDL: PLAN_TYPE, START_DATE, END_DATE, AUTO_RENEW."""
    n = len(members)
    has_churn = churn_dates.notna()
    # auto_renew is True for active members, False for churned
    auto_renew = ~has_churn

    df = pd.DataFrame(
        {
            "member_id": members["member_id"].values,
            "plan_type": members["plan_type"].values,
            "start_date": members["signup_date"].values,
            "end_date": churn_dates.values,
            "auto_renew": auto_renew.values,
        }
    )
    return df


def save_dataframe(df: pd.DataFrame, output_dir: pathlib.Path, name: str) -> None:
    """Persist a DataFrame to CSV."""
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{name}.csv"
    df.to_csv(path, index=False)
    print(f"  {name}.csv: {len(df):,} rows")


def main() -> None:
    parser = argparse.ArgumentParser(description="Synthetic data generator")
    parser.add_argument(
        "--config",
        type=pathlib.Path,
        default=pathlib.Path(__file__).parent / "config.yaml",
        help="Path to configuration YAML",
    )
    parser.add_argument(
        "--output-dir",
        type=pathlib.Path,
        default=pathlib.Path("data"),
        help="Directory to write CSV files",
    )
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    args = parser.parse_args()

    np.random.seed(args.seed)
    Faker.seed(args.seed)
    fake = Faker()
    cfg = load_config(args.config)

    print(
        f"Generating {cfg.member_count:,} members from {cfg.start_date} to {cfg.end_date}"
    )

    members = generate_members(cfg)
    churn_dates = assign_churn_dates(members, cfg)
    print("Generating daily_metrics (vectorized)...")
    daily_metrics = generate_daily_metrics(members, churn_dates, cfg)
    print("Generating feature_events...")
    feature_events = generate_feature_events(members, churn_dates, cfg)
    print("Generating sessions...")
    sessions = generate_sessions(members, churn_dates, cfg)
    experiments = generate_experiments(cfg)
    experiment_assignments = generate_experiment_assignments(members, experiments)
    subscriptions = generate_subscriptions(members, churn_dates)

    print(f"\nWriting to {args.output_dir}/")
    save_dataframe(members, args.output_dir, "members")
    save_dataframe(daily_metrics, args.output_dir, "daily_metrics")
    save_dataframe(feature_events, args.output_dir, "feature_events")
    save_dataframe(sessions, args.output_dir, "sessions")
    save_dataframe(experiments, args.output_dir, "experiments")
    save_dataframe(experiment_assignments, args.output_dir, "experiment_assignments")
    save_dataframe(subscriptions, args.output_dir, "subscriptions")
    print("\nDone.")


if __name__ == "__main__":
    main()
