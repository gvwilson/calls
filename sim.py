#!/usr/bin/env python

"""Call center discrete event simulation."""

import argparse
from datetime import datetime, timedelta
from faker import Faker
import numpy as np
from pathlib import Path
import polars as pl
import asimpy
from asimpy.interrupt import Interrupt
from sqlalchemy import create_engine

SEED = 192738
LOCALE = "et_EE"
NUM_CLIENTS = 5
NUM_AGENTS = 3

CALL_INTERVAL_MU = np.log(8.0)
CALL_INTERVAL_SIGMA = 0.5
CALL_DURATION_MU = 0.2
CALL_FRAC_LONG = 0.2
CALL_MULT_LONG = 2.0

SIMULATION_EPOCH = datetime(2025, 1, 6, 9, 0)
WORK_HOURS_PER_DAY = 8
WORK_DAYS_PER_WEEK = 5
HOURS_PER_DAY = 24
HOURS_PER_WEEK = 7 * HOURS_PER_DAY

# 200 compacted working hours = 5 working weeks = 840 real hours
SIMULATION_WORK_HOURS = 200.0
SIMULATION_TIME = (
    SIMULATION_WORK_HOURS / (WORK_HOURS_PER_DAY * WORK_DAYS_PER_WEEK) * HOURS_PER_WEEK
)

CALL_ID_MISSING_FRAC = 0.05

# Duration recorded for a call that finds no available agent (hours)
CALL_FAILED_DURATION_HOURS = 1 / 60  # one minute

# Time an agent spends on wrap-up after a call before returning to the pool (hours)
AGENT_WRAPUP_TIME = 5 / 60  # five minutes


def main():
    args = parse_args()
    rng = np.random.default_rng(args.seed)
    fake = Faker(locale=LOCALE)

    agents = make_agents(fake)
    clients = make_clients(fake, rng)
    records = simulate(rng, clients, agents)
    mangle_calls(rng, records)

    engine = create_engine(f"sqlite:///{Path(args.db)}")
    with engine.connect() as conn:
        for name, df in (("agent", agents), ("client", clients)):
            df.write_database(name, conn, if_table_exists="replace")
        for name, df in records.items():
            df.write_database(name, conn, if_table_exists="replace")


# ----------------------------------------------------------------------


def hours_to_hms(hours):
    """Convert a duration in fractional hours to an H:MM:SS string."""
    total_seconds = round(hours * 3600)
    h, remainder = divmod(total_seconds, 3600)
    m, s = divmod(remainder, 60)
    return f"{h}:{m:02d}:{s:02d}"


def id_generator(stem, digits):
    """Generate unique IDs of the form 'stemDDDD'."""

    i = 1
    while True:
        temp = str(i)
        assert len(temp) <= digits, f"ID generation overflow {stem}: {i}"
        yield f"{stem}{temp.zfill(digits)}"
        i += 1


def make_agents(fake):
    return make_persons(fake, "A", NUM_AGENTS)


def make_clients(fake, rng):
    result = make_persons(fake, "C", NUM_CLIENTS)
    result = result.with_columns(
        pl.Series(
            "call_interval",
            np.random.lognormal(CALL_INTERVAL_MU, CALL_INTERVAL_SIGMA, result.height),
        )
    )
    num_long_callers = int(CALL_FRAC_LONG * NUM_CLIENTS)
    indices = rng.choice(NUM_CLIENTS, size=num_long_callers, replace=False)
    call_duration = np.full(NUM_CLIENTS, CALL_DURATION_MU)
    call_duration[indices] = CALL_MULT_LONG * CALL_DURATION_MU
    result = result.with_columns(pl.Series("call_duration", call_duration))
    return result


def make_persons(fake, prefix, num):
    id_gen = id_generator(prefix, 4)
    return pl.from_dicts(
        [
            {
                "ident": next(id_gen),
                "family": fake.last_name(),
                "personal": fake.first_name(),
            }
            for i in range(1, num + 1)
        ]
    )


def mangle_calls(rng, records):
    """Add messiness to call data."""
    null_id_indices = rng.random(records["calls"].height) < CALL_ID_MISSING_FRAC
    records["calls"] = records["calls"].with_columns(
        pl.when(pl.Series(null_id_indices))
        .then(None)
        .otherwise(pl.col("caller"))
        .alias("caller"),
    )


def next_work_time(t):
    """Return the earliest time >= t that falls within working hours.

    Simulation time is real hours from SIMULATION_EPOCH (Mon 09:00).
    Working windows within each 168-hour week start at hours 0, 24,
    48, 72, and 96, each spanning 8 hours (09:00–17:00).
    """
    week, pos = divmod(t, HOURS_PER_WEEK)
    for day in range(WORK_DAYS_PER_WEEK):
        day_start = day * HOURS_PER_DAY
        day_end = day_start + WORK_HOURS_PER_DAY
        if pos < day_start:
            return week * HOURS_PER_WEEK + day_start
        if pos < day_end:
            return t  # already within a working window
    # Past Friday 17:00: advance to next Monday 09:00
    return (week + 1) * HOURS_PER_WEEK


def parse_args():
    parser = argparse.ArgumentParser(description="Synthesize call center data via DES")
    parser.add_argument("--db", help="Output database")
    parser.add_argument("--seed", type=int, default=SEED, help="RNG seed")
    return parser.parse_args()


def real_hours_to_datetime(t):
    """Convert real simulation hours to wall-clock datetime, truncated to minute."""
    return (SIMULATION_EPOCH + timedelta(hours=t)).replace(second=0, microsecond=0)


def real_to_compacted(t):
    """Convert real simulation hours to compacted working hours.

    Assumes t falls exactly within a working window (i.e. is a
    call-start time).  Compacted hours match the call_start values
    produced by calls.py.
    """
    week, pos = divmod(t, HOURS_PER_WEEK)
    day = int(pos // HOURS_PER_DAY)
    hour_of_day = pos % HOURS_PER_DAY
    return (week * WORK_DAYS_PER_WEEK + day) * WORK_HOURS_PER_DAY + hour_of_day


# ----------------------------------------------------------------------


class Agent(asimpy.Process):
    """A call center agent who handles calls then enters a wrap-up period.

    Agents start in the shared pool. A Caller removes an agent from
    the pool for the duration of a call, then interrupts the
    agent. The agent waits before returning to the pool.
    """

    def init(self, rng, pool, details):
        self.pool = pool
        self.rng = rng
        self.ident = details["ident"]
        pool.append(self)

    async def run(self):
        while True:
            try:
                await self.timeout(float("inf"))  # idle: wait to be triggered
            except Interrupt:
                pass
            await self.timeout(AGENT_WRAPUP_TIME)
            self.pool.append(self)


class Caller(asimpy.Process):
    """A client who places calls during working hours."""

    def init(self, rng, pool, records, call_id, details):
        self.rng = rng
        self.pool = pool
        self.records = records
        self.call_id = call_id
        self.ident = details["ident"]
        self.call_interval = details["call_interval"]
        self.call_duration_mean = details["call_duration"]

    async def run(self):
        while True:
            # Draw the next inter-call gap and skip forward to the next
            # working window if the gap lands outside working hours.
            interval = self.rng.exponential(self.call_interval)
            next_call = next_work_time(self.now + interval)
            if next_call >= SIMULATION_TIME:
                break
            await self.timeout(next_call - self.now)

            # Take a random agent from the pool; if empty, the call fails.
            if self.pool:
                idx = int(self.rng.integers(len(self.pool)))
                agent = self.pool.pop(idx)
                duration = self.rng.exponential(self.call_duration_mean)
                agent_ident = agent.ident
            else:
                agent = None
                duration = CALL_FAILED_DURATION_HOURS
                agent_ident = None

            self.records["calls"].append(
                {
                    "ident": next(self.call_id),
                    "caller": self.ident,
                    "agent": agent_ident,
                    "call_start": real_to_compacted(self.now),
                    "call_duration": hours_to_hms(duration),
                    "call_start_time": real_hours_to_datetime(self.now),
                }
            )
            await self.timeout(duration)

            if agent is not None:
                agent.interrupt("wrapup")


def simulate(rng, clients, agents_df):
    pool = []
    records = {
        "calls": [],
    }
    call_id = id_generator("X", 6)

    env = asimpy.Environment()
    for row in agents_df.iter_rows(named=True):
        Agent(env, rng, pool, row)
    for row in clients.iter_rows(named=True):
        Caller(env, rng, pool, records, call_id, row)
    env.run(until=SIMULATION_TIME)

    for key, data in records.items():
        records[key] = pl.from_dicts(data)
    return records


# ----------------------------------------------------------------------


if __name__ == "__main__":
    main()
