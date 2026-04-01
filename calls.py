#!/usr/bin/env python

"""Call center data generator."""

import argparse
from datetime import datetime, timedelta
from faker import Faker
import numpy as np
from pathlib import Path
import polars as pl
from sqlalchemy import create_engine


SEED = 192738

LOCALE = "et_EE"
NUM_CLIENTS = 5
NUM_AGENTS = 3

# All times in hours
SIMULATION_TIME = 200.0

# Epoch for wall-clock conversion: a Monday at 09:00
SIMULATION_START = datetime(2025, 1, 6, 9, 0)
WORK_HOURS_PER_DAY = 8   # 09:00–17:00
WORK_DAYS_PER_WEEK = 5   # Mon–Fri

CALL_INTERVAL_MU = np.log(8.0)
CALL_INTERVAL_SIGMA = 0.5

CALL_DURATION_MU = 0.2
CALL_FRAC_LONG = 0.2
CALL_MULT_LONG = 2.0
CALL_ID_MISSING_FRAC = 0.05


def main():
    """Main driver."""

    args = _parse_args()
    rng = np.random.default_rng(args.seed)
    fake = Faker(locale=LOCALE)

    agents = _make_agents(fake)
    clients = _make_clients(fake, rng)
    calls = _make_calls(rng, clients)

    engine = create_engine(f"sqlite:///{Path(args.db)}")
    with engine.connect() as conn:
        for name, df in (("agent", agents), ("client", clients), ("call", calls)):
            df.write_database(name, conn, if_table_exists="replace")


def _hours_to_hms(hours):
    """Convert a duration in fractional hours to an H:MM:SS string."""

    total_seconds = round(hours * 3600)
    h, remainder = divmod(total_seconds, 3600)
    m, s = divmod(remainder, 60)
    return f"{h}:{m:02d}:{s:02d}"


def _make_agents(fake):
    """Create agents."""

    return _make_persons(fake, "A", NUM_AGENTS)


def _make_calls(rng, clients):
    """Create calls made by clients and handled by agents."""

    calls = []
    for cli in clients.iter_rows(named=True):
        current_time = 0.0
        while True:
            current_time += rng.exponential(cli["call_interval"])
            if current_time > SIMULATION_TIME:
                break
            call_duration = rng.exponential(cli["call_duration"])
            calls.append(
                {
                    "caller": cli["ident"],
                    "call_start": current_time,
                    "call_duration": call_duration,
                }
            )

    calls = pl.from_dicts(calls)
    null_id_indices = rng.random(calls.height) < CALL_ID_MISSING_FRAC
    calls = calls.with_columns(
        pl.when(pl.Series(null_id_indices)).then(None).otherwise(pl.col("caller")).alias("caller"),
        pl.Series(
            "call_start_time",
            [_sim_hours_to_datetime(h) for h in calls["call_start"]],
        ),
        pl.Series(
            "call_duration",
            [_hours_to_hms(h) for h in calls["call_duration"]],
        ),
    )

    return calls


def _make_clients(fake, rng):
    """Create clients."""

    result = _make_persons(fake, "C", NUM_CLIENTS)

    result = result.with_columns(
        pl.Series(
            "call_interval",
            np.random.lognormal(CALL_INTERVAL_MU, CALL_INTERVAL_SIGMA, result.height),
        )
    )

    num_long_callers = int(CALL_FRAC_LONG * NUM_CLIENTS)
    indices = rng.choice(NUM_CLIENTS, size=num_long_callers, replace=False)
    call_duration = np.full(NUM_CLIENTS, CALL_DURATION_MU)
    call_duration[indices] = 2 * CALL_DURATION_MU
    result = result.with_columns(pl.Series("call_duration", call_duration))

    return result


def _make_persons(fake, prefix, num):
    """Create persons with IDs."""

    return pl.from_dicts(
        [
            {
                "ident": f"{prefix}{i:04d}",
                "family": fake.last_name(),
                "personal": fake.first_name(),
            }
            for i in range(1, num + 1)
        ]
    )


def _parse_args():
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description="Synthesize call center data")
    parser.add_argument("--db", help="Output database")
    parser.add_argument("--seed", type=int, default=SEED, help="RNG seed")
    return parser.parse_args()


def _sim_hours_to_datetime(hours):
    """Convert compacted simulation hours to a real wall-clock datetime.

    Simulation time is packed from 09:00-17:00 Mon-Fri, so each
    simulated hour maps to one working hour. Fractional hours are
    preserved, so sub-hour precision is retained.
    """
    work_hours_per_week = WORK_HOURS_PER_DAY * WORK_DAYS_PER_WEEK  # 40
    weeks, within_week = divmod(hours, work_hours_per_week)
    days, within_day = divmod(within_week, WORK_HOURS_PER_DAY)
    return (
        SIMULATION_START + timedelta(weeks=int(weeks), days=int(days), hours=within_day)
    ).replace(second=0, microsecond=0)


if __name__ == "__main__":
    main()
