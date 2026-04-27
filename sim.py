#!/usr/bin/env python

"""Call center discrete event simulation."""

import altair as alt
import argparse
from datetime import datetime, timedelta
from faker import Faker
import numpy as np
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
OVERLOAD_CLIENT_MULTIPLIER = 3.0  # multiple of existing clients added during overload

# Range for each client's personal mean call duration (minutes).
CALL_DURATION_MEAN_MIN = 3   # lower bound of personal mean call duration
CALL_DURATION_MEAN_MAX = 15  # upper bound of personal mean call duration

# Range for agent followup times (minutes).
CALL_FOLLOWUP_MIN = 10  # minimum followup time; also lower bound of personal max
CALL_FOLLOWUP_MAX_MAX = 60  # upper bound of personal followup max

# Duration of a call that fails to connect (uniformly distributed, in minutes).
CALL_FAILED_DURATION_MIN = 0.5   # 30 seconds
CALL_FAILED_DURATION_MAX = 1.0   # 60 seconds; calls intended shorter than this also fail

# Range for each client's personal maximum inter-call interval (minutes).
CALL_INTERVAL_MIN = 5  # minimum inter-call interval
CALL_INTERVAL_MAX_MIN = 60  # 1 hour
CALL_INTERVAL_MAX_MAX = 240  # 4 hours

# Agent satisfaction rating parameters.
AGENT_BASELINE_MIN = 3.0  # minimum baseline rating an agent can have
AGENT_BASELINE_MAX = 5.0  # maximum baseline rating an agent can have
FATIGUE_INCREMENT = 1.0  # fatigue added per completed call
FATIGUE_DECAY = 0.98  # per-minute multiplicative fatigue decay during followup
RATING_FAILED_CALL = 1  # rating assigned when no agent is available

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

    agents, clients = post_process(world, agents, clients, records)
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
    chart = alt.vconcat(
        alt.hconcat(
            plot_missed_calls_day(shock, records),
            plot_missed_calls_compressed(shock, records),
        ),
        alt.hconcat(
            plot_ratings_over_time(shock, records),
            plot_call_duration_by_client(shock, records),
        ),
        alt.hconcat(
            plot_agent_utilization(shock, records),
            plot_cumulative_missed_calls(shock, records),
        ),
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


def plot_ratings_over_time(shock, records):
    """Plot mean satisfaction rating per day over real wall-clock time."""
    calls = (
        records["calls"]
        .with_columns(pl.col("call_start").dt.truncate("1d").alias("day_start"))
        .group_by("day_start")
        .agg(pl.col("rating").mean().alias("mean_rating"))
        .sort("day_start")
    )
    return (
        alt.Chart(calls)
        .mark_line(point=True)
        .encode(
            x=alt.X("day_start:T", title="date"),
            y=alt.Y(
                "mean_rating:Q",
                title="mean rating",
                scale=alt.Scale(domain=[1, 5]),
            ),
            tooltip=[
                alt.Tooltip("day_start:T", title="date", format="%Y-%m-%d"),
                alt.Tooltip("mean_rating:Q", title="mean rating", format=".2f"),
            ],
        )
        .properties(
            title=f"{shock}: mean satisfaction rating per day", width=PLOT_WIDTH
        )
    )


def add_working_slot(df, time_col):
    """Add a working_slot column (index of working hour) derived from a datetime column."""
    return (
        df.with_columns(
            (pl.col(time_col) - pl.lit(SIMULATION_START))
            .dt.total_minutes()
            .alias("_msince")
        )
        .with_columns(
            (pl.col("_msince") // MINUTES_PER_WEEK).alias("_week"),
            ((pl.col("_msince") % MINUTES_PER_WEEK) // MINUTES_PER_DAY).alias("_day"),
            ((pl.col("_msince") % MINUTES_PER_DAY) // 60).alias("_hour"),
        )
        .filter(
            (pl.col("_day") < WORK_DAYS_PER_WEEK)
            & (pl.col("_hour") < WORK_HOURS_PER_DAY)
        )
        .with_columns(
            (
                pl.col("_week") * WORK_DAYS_PER_WEEK * WORK_HOURS_PER_DAY
                + pl.col("_day") * WORK_HOURS_PER_DAY
                + pl.col("_hour")
            ).alias("working_slot")
        )
        .drop(["_msince", "_week", "_day", "_hour"])
    )


def plot_call_duration_by_client(shock, records):
    """Box plot of call duration per client for successful calls."""
    calls = records["calls"].filter(pl.col("agent_id").is_not_null())
    return (
        alt.Chart(calls)
        .mark_boxplot()
        .encode(
            x=alt.X("client_id:N", title="client"),
            y=alt.Y("call_duration:Q", title="duration (minutes)"),
        )
        .properties(title=f"{shock}: call duration by client", width=PLOT_WIDTH)
    )


def plot_agent_utilization(shock, records):
    """Stacked bar of agent-minutes in calls, followup, and idle per working hour."""
    total_slots = NUM_WEEKS * WORK_DAYS_PER_WEEK * WORK_HOURS_PER_DAY
    all_slots = pl.DataFrame({"working_slot": list(range(total_slots))})

    call_slots = (
        add_working_slot(
            records["calls"].filter(pl.col("agent_id").is_not_null()), "call_start"
        )
        .group_by("working_slot")
        .agg(pl.col("call_duration").sum().alias("call_min"))
    )
    combined = all_slots.join(call_slots, on="working_slot", how="left")

    if not records["followups"].is_empty():
        followup_slots = (
            add_working_slot(records["followups"], "followup_start")
            .group_by("working_slot")
            .agg(pl.col("followup_duration").sum().alias("followup_min"))
        )
        combined = combined.join(followup_slots, on="working_slot", how="left")
    else:
        combined = combined.with_columns(pl.lit(0.0).alias("followup_min"))

    combined = (
        combined.fill_null(0.0)
        .with_columns(
            (
                pl.lit(float(NUM_AGENTS) * 60)
                - pl.col("call_min")
                - pl.col("followup_min")
            ).alias("idle_min")
        )
        .with_columns(
            pl.when(pl.col("idle_min") < 0)
            .then(0.0)
            .otherwise(pl.col("idle_min"))
            .alias("idle_min")
        )
    )

    long = pl.concat([
        combined.select("working_slot", pl.col("call_min").alias("minutes"))
        .with_columns(pl.lit("call").alias("activity")),
        combined.select("working_slot", pl.col("followup_min").alias("minutes"))
        .with_columns(pl.lit("followup").alias("activity")),
        combined.select("working_slot", pl.col("idle_min").alias("minutes"))
        .with_columns(pl.lit("idle").alias("activity")),
    ])

    return (
        alt.Chart(long)
        .mark_bar()
        .encode(
            x=alt.X("working_slot:Q", title="working hour"),
            y=alt.Y("minutes:Q", title="agent-minutes", stack=True),
            color=alt.Color(
                "activity:N",
                scale=alt.Scale(domain=["call", "followup", "idle"]),
                legend=alt.Legend(title="agent time"),
            ),
            tooltip=[
                alt.Tooltip("working_slot:Q", title="working hour"),
                alt.Tooltip("activity:N", title="activity"),
                alt.Tooltip("minutes:Q", title="minutes", format=".1f"),
            ],
        )
        .properties(title=f"{shock}: agent utilization", width=PLOT_WIDTH)
    )


def plot_cumulative_missed_calls(shock, records):
    """Line chart of cumulative missed calls over working hours."""
    calls = (
        add_working_slot(
            records["calls"].filter(pl.col("agent_id").is_null()), "call_start"
        )
        .group_by("working_slot")
        .agg(pl.len().alias("missed"))
        .sort("working_slot")
        .with_columns(pl.col("missed").cum_sum().alias("cumulative_missed"))
    )
    return (
        alt.Chart(calls)
        .mark_line()
        .encode(
            x=alt.X("working_slot:Q", title="working hour"),
            y=alt.Y("cumulative_missed:Q", title="cumulative missed calls"),
            tooltip=[
                alt.Tooltip("working_slot:Q", title="working hour"),
                alt.Tooltip("cumulative_missed:Q", title="cumulative missed"),
            ],
        )
        .properties(title=f"{shock}: cumulative missed calls", width=PLOT_WIDTH)
    )


def post_process(world, agents, clients, records):
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

    # Remove simulation parameters not needed in output.
    agents = agents.drop(["call_followup_max", "baseline_rating"])
    clients = clients.drop(["call_interval_max", "call_duration_mean"])

    return agents, clients


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
    the pool for the duration of a call, then interrupts the agent.
    The agent accumulates fatigue with each call, which decays during
    followup. Higher fatigue lowers the satisfaction rating clients give.
    """

    _all = []

    def init(self, world, details):
        self.world = world
        self.ident = details["ident"]
        self.call_followup_max = details["call_followup_max"]
        self.baseline_rating = details["baseline_rating"]
        self.fatigue = 0.0
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
                self.fatigue += FATIGUE_INCREMENT
                await self.timeout(followup_duration)
                # Fatigue decays exponentially with time spent in followup.
                self.fatigue *= FATIGUE_DECAY**followup_duration
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
        self.call_duration_mean = details["call_duration_mean"]
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
                    intended_duration = self.world.rng.exponential(self.call_duration_mean)
                    if self.world.pool and intended_duration >= CALL_FAILED_DURATION_MAX:
                        idx = int(self.world.rng.integers(len(self.world.pool)))
                        agent = self.world.pool.pop(idx)
                        agent_id = agent.ident
                        # Rating is based on the agent's fatigue before this call.
                        rating = max(
                            1, min(5, round(agent.baseline_rating - agent.fatigue))
                        )
                        call_duration = intended_duration
                        await self.timeout(call_duration)
                        call_end = minutes_to_datetime(self.now)
                    else:
                        agent = None
                        agent_id = None
                        rating = RATING_FAILED_CALL
                        call_duration = self.world.rng.uniform(
                            CALL_FAILED_DURATION_MIN, CALL_FAILED_DURATION_MAX
                        )
                        call_end = minutes_to_datetime(self.now + call_duration)
                    self.world.calls.append(
                        {
                            "client_id": self.ident,
                            "call_id": call_id,
                            "call_start": call_start,
                            "call_duration": call_duration,
                            "call_end": call_end,
                            "agent_id": agent_id,
                            "rating": rating,
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

            case "overload":
                # Flood the center with new clients to sustain agent fatigue.
                num_new = int(OVERLOAD_CLIENT_MULTIPLIER * NUM_CLIENTS)
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
        ),
        pl.Series(
            "baseline_rating",
            world.rng.uniform(AGENT_BASELINE_MIN, AGENT_BASELINE_MAX, num),
        ),
    )
    return result


def make_clients(world, num):
    result = make_persons(world.fake, world.client_id, num)
    result = result.with_columns(
        pl.Series(
            "call_interval_max",
            world.rng.uniform(CALL_INTERVAL_MAX_MIN, CALL_INTERVAL_MAX_MAX, num),
        ),
        pl.Series(
            "call_duration_mean",
            world.rng.uniform(CALL_DURATION_MEAN_MIN, CALL_DURATION_MEAN_MAX, num),
        ),
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
        choices=["followup", "newclients", "overload", "plain", "special"],
        help="shocks to the system",
    )
    return parser.parse_args()


# ----------------------------------------------------------------------


if __name__ == "__main__":
    main()
