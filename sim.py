#!/usr/bin/env python

"""Call center discrete event simulation."""

import altair as alt
import argparse
from datetime import datetime, timedelta
from faker import Faker
import numpy as np
from pathlib import Path
import polars as pl
from asimpy import Environment, Interrupt, Process
from sqlalchemy import create_engine

# Default random number generation seed.
SEED = 192738

# Locale for personal names.
LOCALE = "et_EE"

# Initial number of people.
NUM_AGENTS = 2
NUM_CLIENTS = 5

# Duration of simulation.
SIMULATION_START = datetime(2025, 1, 6, 9, 0)
NUM_WEEKS = 6
WORK_HOURS_PER_DAY = 8
WORK_DAYS_PER_WEEK = 5

# Fraction of call records missing the client ID.
CALL_ID_MISSING_FRAC = 0.05

# Shock multipliers.
FOLLOWUP_MULTIPLIER = 5.0  # how much agent followup time increases
NEW_CLIENT_FRAC = 1.0  # fraction of existing clients added as new clients
SPECIAL_MULTIPLIER = 5.0  # how much call frequency increases during special offer

# Range for agent followup times (minutes).
CALL_FOLLOWUP_MIN = 10  # minimum followup time; also lower bound of personal max
CALL_FOLLOWUP_MAX_MAX = 60  # upper bound of personal followup max

# Duration recorded for a call that finds no available agent (minutes).
CALL_FAILED_DURATION = 1

# Range for each client's personal maximum inter-call interval (minutes).
CALL_INTERVAL_MIN = 5  # minimum inter-call interval
CALL_INTERVAL_MAX_MIN = 60  # 1 hour
CALL_INTERVAL_MAX_MAX = 240  # 4 hours

# Time conversions (minutes).
MINUTES_PER_DAY = 24 * 60
MINUTES_PER_WEEK = 7 * MINUTES_PER_DAY
WORK_DAY_MINUTES = WORK_HOURS_PER_DAY * 60

# Total simulation duration in real minutes.
SIMULATION_TIME = NUM_WEEKS * MINUTES_PER_WEEK


# Display properties.
PLOT_WIDTH = 450

# ----------------------------------------------------------------------


def main():
    args = parse_args()

    world = World(args.seed, args.shock)
    agents = make_agents(world, NUM_AGENTS)
    clients = make_clients(world, NUM_CLIENTS)
    records = simulate(world, clients, agents)

    clients = post_process(world, clients, records)
    make_db(args.shock, agents, clients, records)
    plot_all(args.shock, records)


def make_db(shock, agents, clients, records):
    """Create database."""
    engine = create_engine(f"sqlite:///{shock}.db")
    with engine.connect() as conn:
        for name, df in (("agent", agents), ("client", clients)):
            df.write_database(name, conn, if_table_exists="replace")
        for name, df in records.items():
            if df.is_empty():
                continue
            df.write_database(name, conn, if_table_exists="replace")


def plot_all(shock, records):
    """Create and save all plots for a scenario as a single HTML page."""
    chart = alt.hconcat(
        plot_missed_calls_day(shock, records),
        plot_missed_calls_compressed(shock, records),
    )
    chart.save(f"{shock}.html")


def plot_missed_calls_day(shock, records):
    """Plot missed calls per hour against real wall-clock time."""
    calls = (
        records["calls"]
        .filter(pl.col("agent_id").is_null())
        .with_columns(pl.col("call_start").dt.truncate("1h").alias("hour_start"))
        .group_by("hour_start")
        .agg(pl.len().alias("missed"))
        .sort("hour_start")
    )
    return (
        alt.Chart(calls)
        .mark_bar()
        .encode(
            x=alt.X("hour_start:T", title="time"),
            y=alt.Y("missed:Q", title="missed calls"),
            tooltip=[
                alt.Tooltip(
                    "hour_start:T", title="hour starting", format="%Y-%m-%d %H:%M"
                ),
                alt.Tooltip("missed:Q", title="missed calls"),
            ],
        )
        .properties(
            title=f"{shock}: missed calls per hour (real time)", width=PLOT_WIDTH
        )
    )


def plot_missed_calls_compressed(shock, records):
    """Plot missed calls per working hour on a compressed X axis with no gaps."""
    calls = (
        records["calls"]
        .filter(pl.col("agent_id").is_null())
        .with_columns(
            pl.col("call_start").dt.truncate("1h").alias("hour_start"),
            (pl.col("call_start") - pl.lit(SIMULATION_START))
            .dt.total_minutes()
            .alias("minutes_since_start"),
        )
        .with_columns(
            (pl.col("minutes_since_start") // MINUTES_PER_WEEK).alias("week"),
            (
                (pl.col("minutes_since_start") % MINUTES_PER_WEEK) // MINUTES_PER_DAY
            ).alias("day_in_week"),
            ((pl.col("minutes_since_start") % MINUTES_PER_DAY) // 60).alias(
                "hour_in_day"
            ),
        )
        .filter(
            (pl.col("day_in_week") < WORK_DAYS_PER_WEEK)
            & (pl.col("hour_in_day") < WORK_HOURS_PER_DAY)
        )
        .with_columns(
            (
                pl.col("week") * WORK_DAYS_PER_WEEK * WORK_HOURS_PER_DAY
                + pl.col("day_in_week") * WORK_HOURS_PER_DAY
                + pl.col("hour_in_day")
            ).alias("working_slot")
        )
        .group_by("working_slot", "hour_start")
        .agg(pl.len().alias("missed"))
        .sort("working_slot")
    )
    return (
        alt.Chart(calls)
        .mark_bar()
        .encode(
            x=alt.X("working_slot:Q", title="working hour"),
            y=alt.Y("missed:Q", title="missed calls"),
            tooltip=[
                alt.Tooltip(
                    "hour_start:T", title="hour starting", format="%Y-%m-%d %H:%M"
                ),
                alt.Tooltip("missed:Q", title="missed calls"),
            ],
        )
        .properties(title=f"{shock}: missed calls per hour (working time)", width=600)
    )


def post_process(world, clients, records):
    """Tidy up data."""

    # Mangle calls.
    null_id_indices = world.rng.random(records["calls"].height) < CALL_ID_MISSING_FRAC
    records["calls"] = records["calls"].with_columns(
        pl.when(pl.Series(null_id_indices))
        .then(None)
        .otherwise(pl.col("client_id"))
        .alias("client_id"),
    )

    # Add extra clients.
    if world.more_clients is not None:
        clients = pl.concat([clients, world.more_clients])

    return clients


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


class Agent(Process):
    """A call center agent who handles calls then enters a wrap-up period.

    Agents start in the shared pool. A Client removes an agent from
    the pool for the duration of a call, then interrupts the
    agent. The agent waits before returning to the pool.
    """

    _all = []

    def init(self, world, details):
        self.world = world
        self.ident = details["ident"]
        self.call_followup_max = details["call_followup_max"]
        Agent._all.append(self)
        self.world.pool.append(self)

    async def run(self):
        while True:
            try:
                await self.timeout(float("inf"))
            except Interrupt as wakeup:
                call_id = wakeup.cause
                followup_start = minutes_to_datetime(self.now)
                followup_duration = self.world.rng.uniform(
                    CALL_FOLLOWUP_MIN, self.call_followup_max
                )
                await self.timeout(followup_duration)
                self.world.followups.append(
                    {
                        "agent_id": self.ident,
                        "call_id": call_id,
                        "followup_start": followup_start,
                        "followup_duration": followup_duration,
                        "followup_end": minutes_to_datetime(self.now),
                    }
                )
                self.world.pool.append(self)


class Client(Process):
    """A client who places calls during working hours."""

    _all = []

    def init(self, world, details):
        self.world = world
        self.ident = details["ident"]
        self.call_interval_max = details["call_interval_max"]
        Client._all.append(self)

    async def run(self):
        for week in range(NUM_WEEKS):
            for day in range(WORK_DAYS_PER_WEEK):
                day_start = week * MINUTES_PER_WEEK + day * MINUTES_PER_DAY
                day_end = day_start + WORK_DAY_MINUTES
                if day_end <= self.now:
                    continue
                if day_start > self.now:
                    await self.timeout(day_start - self.now)
                while True:
                    interval = self.world.rng.uniform(
                        CALL_INTERVAL_MIN, self.call_interval_max
                    )
                    if self.now + interval >= day_end:
                        break
                    await self.timeout(interval)
                    call_id = next(self.world.call_id)
                    call_start = minutes_to_datetime(self.now)
                    if self.world.pool:
                        idx = int(self.world.rng.integers(len(self.world.pool)))
                        agent = self.world.pool.pop(idx)
                        agent_id = agent.ident
                    else:
                        agent = None
                        agent_id = None
                    if agent is None:
                        call_duration = CALL_FAILED_DURATION
                        call_end = minutes_to_datetime(self.now + CALL_FAILED_DURATION)
                    else:
                        call_duration = 0
                        call_end = call_start
                    self.world.calls.append(
                        {
                            "client_id": self.ident,
                            "call_id": call_id,
                            "call_start": call_start,
                            "call_duration": call_duration,
                            "call_end": call_end,
                            "agent_id": agent_id,
                        }
                    )
                    if agent is not None:
                        agent.interrupt(call_id)


class Shock(Process):
    """Applies system-wide shock to the simulation at scheduled time."""

    def init(self, world):
        self.world = world

    async def run(self):
        await self.timeout(SIMULATION_TIME / 2)
        match self.world.shock:
            case "plain":
                pass

            case "followup":
                for agent in Agent._all:
                    agent.call_followup_max *= FOLLOWUP_MULTIPLIER

            case "newclients":
                num_new = int(NEW_CLIENT_FRAC * NUM_CLIENTS)
                self.world.more_clients = make_clients(self.world, num_new)
                for row in self.world.more_clients.iter_rows(named=True):
                    Client(self._env, self.world, row)

            case "special":
                for client in Client._all:
                    client.call_interval_max /= SPECIAL_MULTIPLIER
                await self.timeout(SIMULATION_TIME / 4)
                for client in Client._all:
                    client.call_interval_max *= SPECIAL_MULTIPLIER

            case _:
                raise ValueError(f"unknown shock {self.world.shock}")


def simulate(world, clients, agents):
    env = Environment()
    for row in agents.iter_rows(named=True):
        Agent(env, world, row)
    for row in clients.iter_rows(named=True):
        Client(env, world, row)
    Shock(env, world)
    env.run(until=SIMULATION_TIME)

    followups = pl.from_dicts(world.followups) if world.followups else pl.DataFrame()
    return {
        "calls": pl.from_dicts(world.calls),
        "followups": followups,
    }


# ----------------------------------------------------------------------


def minutes_to_datetime(t):
    """Convert simulation minutes to wall-clock datetime, truncated to minute."""
    return (SIMULATION_START + timedelta(minutes=t)).replace(second=0, microsecond=0)


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
            "call_followup_max",
            world.rng.uniform(CALL_FOLLOWUP_MIN, CALL_FOLLOWUP_MAX_MAX, num),
        )
    )
    return result


def make_clients(world, num):
    result = make_persons(world.fake, world.client_id, num)
    result = result.with_columns(
        pl.Series(
            "call_interval_max",
            world.rng.uniform(CALL_INTERVAL_MAX_MIN, CALL_INTERVAL_MAX_MAX, num),
        )
    )
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


def parse_args():
    parser = argparse.ArgumentParser(description="Synthesize call center data via DES")
    parser.add_argument("--seed", type=int, default=SEED, help="RNG seed")
    parser.add_argument(
        "--shock",
        default="plain",
        choices=["followup", "newclients", "plain", "special"],
        help="shocks to the system",
    )
    return parser.parse_args()


# ----------------------------------------------------------------------


if __name__ == "__main__":
    main()
