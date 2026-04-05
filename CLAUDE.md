# Claude

This project is a simple simulation of a call center in Python using
the `asimpy` package. Its purpose is to synthesize data that can be
used in teaching people how to write SQL queries and visualize their
results. The basic scenario models clients and agents whose behavior
does not change over time. Three other scenarios model changes in
behavior.

## Interaction

-   Develop a plan and present it for approval before making changes.

## Structure

-   The old simulation is in `sim.py` and is not to modified.
-   The new simulation that we are writing goes in `new.py`.
-   Other files:
    -   `README.md`: package home page (do not modify).
    -   `LICENSE.md`: license (do not modify).
    -   `CODE_OF_CONDUCT.md`: code of conduct (do not modify).
-   This project has a `uv` virtual environment, so use `python` rather than `python3` to run commands.

## Build and Test Commands

-   Repeatable actions are saved in `Makefile`.
-   For now, use `python new.py` to run the new simulation.

## Style Rules

-   Do not use type annotations.
-   Keep docstrings short and do not bother to document parameters.
-   Use named constants declared at the top of the file rather than magic numbers.
-   Put all imports at the top of the file.
-   Use minutes for all calculations.
-   Store datetime objects in the SQLite database. If durations must be stored, store them in minutes.
