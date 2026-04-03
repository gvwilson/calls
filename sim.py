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

# Default random number generation seed.
SEED = 192738

# Locale for personal names.
LOCALE = "et_EE"

# (Initial) number of clients.
NUM_CLIENTS = 5

# (Initial) number of agents.
NUM_AGENTS = 3

# Duration of simulation.
SIMULATION_START = datetime(2025, 1, 6, 9, 0)
WORK_HOURS_PER_DAY = 8
WORK_DAYS_PER_WEEK = 5
HOURS_PER_DAY = 24
HOURS_PER_WEEK = 7 * HOURS_PER_DAY

# 200 compacted working hours.
SIMULATION_WORK_HOURS = 200.0
SIMULATION_TIME = (
    SIMULATION_WORK_HOURS / (WORK_HOURS_PER_DAY * WORK_DAYS_PER_WEEK) * HOURS_PER_WEEK
)

# Parameters for generating per-client intervals between calls.
CALL_INTERVAL_MU = np.log(8.0)
CALL_INTERVAL_SIGMA = 0.5

# Parameter for generating call duration.
CALL_DURATION_MU = 0.2

# Fraction of callers who create long calls, and how much longer those calls are.
CALL_FRAC_LONG = 0.2
CALL_MULT_LONG = 2.0

# Mean time agents need to follow up after a call completes.
FOLLOWUP_DURATION_MU = 10 / 60 # 5 minutes

# Fraction of call records missing the client ID.
CALL_ID_MISSING_FRAC = 0.05

# Duration recorded for a call that finds no available agent.
CALL_FAILED_DURATION_HOURS = 1 / 60

# How much the client pool increases if new clients are added.
NEW_CLIENT_FRAC = 0.5

# How much agent response time increases.
FOLLOWUP_MULTIPLIER = 2.0

# How much automation decreases call duration.
AUTOMATION_EFFECT = 0.5

# How much call frequency changes when there's a special offer.
SPECIAL_MULTIPLIER = 2.0

# ----------------------------------------------------------------------


def main():
    args = parse_args()

    world = World(args.seed, args.shock)
    agents = make_agents(world, NUM_AGENTS)
    clients = make_clients(world, NUM_CLIENTS)
    records = simulate(world, clients, agents)

    if world.more_clients is not None:
        clients = pl.concat([clients, world.more_clients])
    mangle_calls(world, records)

    engine = create_engine(f"sqlite:///{Path(args.db)}")
    with engine.connect() as conn:
        for name, df in (("agent", agents), ("client", clients)):
            df.write_database(name, conn, if_table_exists="replace")
        for name, df in records.items():
            df.write_database(name, conn, if_table_exists="replace")


# ----------------------------------------------------------------------


class World:
    """Hold simulation artefacts."""
    def __init__(self, seed, shock):
        self.rng = np.random.default_rng(seed)
        self.shock = shock
        self.fake = Faker(locale=LOCALE)
        self.pool = []
        self.calls = []
        self.followups = []
        self.client_id = id_generator("C", 4)
        self.agent_id = id_generator("A", 4)
        self.call_id = id_generator("X", 6)
        self.more_clients = None


class Agent(asimpy.Process):
    """A call center agent who handles calls then enters a wrap-up period.

    Agents start in the shared pool. A Client removes an agent from
    the pool for the duration of a call, then interrupts the
    agent. The agent waits before returning to the pool.
    """

    _all = []

    def init(self, world, details):
        self.world = world
        self.ident = details["ident"]
        self.followup_time = details["followup_time"]
        Agent._all.append(self)
        self.world.pool.append(self)

    async def run(self):
        while True:
            try:
                await self.timeout(float("inf"))  # idle: wait to be triggered
            except Interrupt as wakeup:
                start_time = self.now
                await self.timeout(simple_uniform(self.world.rng, self.followup_time, None))
                self.world.followups.append(
                    {
                        "ident": wakeup.cause,
                        "agent_id": self.ident,
                        "followup_start": real_hours_to_datetime(start_time),
                        "followup_end": real_hours_to_datetime(self.now),
                    }
                )
                self.world.pool.append(self)


class Client(asimpy.Process):
    """A client who places calls during working hours."""

    _all = []

    def init(self, world, details):
        self.world = world
        self.ident = details["ident"]
        self.call_interval = details["call_interval"]
        self.call_duration_mean = details["call_duration"]
        Client._all.append(self)

    async def run(self):
        while True:
            # Draw the next inter-call gap and skip forward to the next
            # working window if the gap lands outside working hours.
            interval = self.world.rng.exponential(self.call_interval)
            next_call = next_work_time(self.now + interval)
            if next_call >= SIMULATION_TIME:
                break
            await self.timeout(next_call - self.now)

            # Take a random agent from the pool; if empty, the call fails.
            if self.world.pool:
                idx = int(self.world.rng.integers(len(self.world.pool)))
                agent = self.world.pool.pop(idx)
                duration = self.world.rng.exponential(self.call_duration_mean)
                agent_ident = agent.ident
            else:
                agent = None
                duration = CALL_FAILED_DURATION_HOURS
                agent_ident = None

            call_id = next(self.world.call_id)
            self.world.calls.append(
                {
                    "ident": call_id,
                    "client_id": self.ident,
                    "agent_id": agent_ident,
                    "call_start": real_to_compacted(self.now),
                    "call_duration": hours_to_hms(duration),
                    "call_start_time": real_hours_to_datetime(self.now),
                }
            )
            await self.timeout(duration)

            if agent is not None:
                agent.interrupt(call_id)


class Shock(asimpy.Process):
    """Applies system-wide shock to the simulation at scheduled time."""

    def init(self, world):
        self.world = world

    async def run(self):
        await self.timeout(SIMULATION_TIME / 2)
        match self.world.shock:
            case None:
                pass

            case "automation":
                for client in Client._all:
                    client.call_duration_mean *= AUTOMATION_EFFECT

            case "followup":
                for agent in Agent._all:
                    agent.followup_time *= FOLLOWUP_MULTIPLIER

            case "newclients":
                num_new_clients = int(NEW_CLIENT_FRAC * NUM_CLIENTS)
                self.world.more_clients = make_clients(self.world, num_new_clients)
                for row in self.world.more_clients.iter_rows(named=True):
                    Client(self._env, self.world, row)

            case "special":
                for client in Client._all:
                    client.call_interval *= SPECIAL_MULTIPLIER
                await self.timeout(SIMULATION_TIME / 4)
                for client in Client._all:
                    client.call_interval /= SPECIAL_MULTIPLIER

            case _:
                raise ValueError(f"unknown shock {self.shock}")


def simulate(world, clients, agents):
    env = asimpy.Environment()
    for row in agents.iter_rows(named=True):
        Agent(env, world, row)
    for row in clients.iter_rows(named=True):
        Client(env, world, row)
    Shock(env, world)
    env.run(until=SIMULATION_TIME)

    return {
        "calls": pl.from_dicts(world.calls),
        "followups": pl.from_dicts(world.followups),
    }


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


def make_agents(world, num):
    result = make_persons(world.fake, world.agent_id, num)
    result = result.with_columns(
        pl.Series(
            "followup_time",
            simple_uniform(world.rng, FOLLOWUP_DURATION_MU, result.height),
        )
    )
    return result


def make_clients(world, num):
    result = make_persons(world.fake, world.client_id, num)
    result = result.with_columns(
        pl.Series(
            "call_interval",
            world.rng.lognormal(CALL_INTERVAL_MU, CALL_INTERVAL_SIGMA, result.height),
        )
    )
    num_long_callers = int(CALL_FRAC_LONG * num)
    indices = world.rng.choice(num, size=num_long_callers, replace=False)
    call_duration = np.full(num, CALL_DURATION_MU)
    call_duration[indices] = CALL_MULT_LONG * CALL_DURATION_MU
    result = result.with_columns(pl.Series("call_duration", call_duration))
    return result


def make_persons(fake, id_gen, num):
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


def mangle_calls(world, records):
    """Add messiness to call data."""
    null_id_indices = world.rng.random(records["calls"].height) < CALL_ID_MISSING_FRAC
    records["calls"] = records["calls"].with_columns(
        pl.when(pl.Series(null_id_indices))
        .then(None)
        .otherwise(pl.col("client_id"))
        .alias("client_id"),
    )


def next_work_time(t):
    """Return the earliest time >= t that falls within working hours.

    Simulation time is real hours from SIMULATION_START.  Working
    windows within each 168-hour week start at hours 0, 24, 48, 72,
    and 96, each spanning 8 hours (09:00–17:00).
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
    parser.add_argument(
        "--shock",
        default=None,
        choices=["automation", "followup", "newclients", "special"],
        help="shocks to the system"
    )
    return parser.parse_args()


def real_hours_to_datetime(t):
    """Convert real simulation hours to wall-clock datetime, truncated to minute."""
    return (SIMULATION_START + timedelta(hours=t)).replace(second=0, microsecond=0)


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


def simple_uniform(rng, mean, length):
    """Generate uniform between 0.5*mean and 1.5*mean."""
    return rng.uniform(0.5 * mean, 1.5 * mean, length)


# ----------------------------------------------------------------------


if __name__ == "__main__":
    main()
