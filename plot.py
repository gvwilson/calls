import polars as pl
import altair as alt

# Example Polars DataFrame
df = pl.DataFrame({
    "call_id": [1, 2, 3],
    "call_start_time": [
        "2026-04-02 09:00:00",
        "2026-04-02 09:30:00",
        "2026-04-02 10:15:00",
    ],
    "call_duration": [300, 900, 600],  # seconds
})

# Prepare data
df = df.with_columns([
    pl.col("call_start_time").str.strptime(pl.Datetime),
    (pl.col("call_start_time") + pl.duration(seconds=pl.col("call_duration")))
        .alias("call_end_time")
])

# Altair chart (directly using Polars DataFrame)
chart = alt.Chart(df).mark_rule().encode(
    x=alt.X("call_start_time:T", title="Start Time"),
    x2="call_end_time:T",
    y=alt.Y("call_id:O", title="Call ID"),
    tooltip=[
        "call_id",
        "call_start_time:T",
        "call_end_time:T"
    ]
).properties(
    width=600,
    height=300
)

chart
