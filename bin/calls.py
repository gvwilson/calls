#!/usr/bin/env python

"""Call center data generator."""

import argparse
from faker import Faker
import numpy as np
from pathlib import Path
import polars as pl
from sqlalchemy import create_engine
import sqlite3

SEED = 192738

LOCALE = "et_EE"
NUM_CLIENTS = 5
NUM_AGENTS = 3

# All times in hours
SIMULATION_TIME = 200.0

CALL_INTERVAL_MU = np.log(8.0)
CALL_INTERVAL_SIGMA = 0.5

CALL_DURATION_MU = 0.2
CALL_FRAC_LONG = 0.2
CALL_MULT_LONG = 2.0


def main():
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


def _make_agents(fake):
    return _make_persons(fake, "A", NUM_AGENTS)


def _make_calls(rng, clients):
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
    return pl.from_dicts(calls)


def _make_clients(fake, rng):
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
    parser = argparse.ArgumentParser(description="Synthesize call center data")
    parser.add_argument("--db", help="Output database")
    parser.add_argument("--seed", type=int, default=SEED, help="RNG seed")
    return parser.parse_args()


if __name__ == "__main__":
    main()
