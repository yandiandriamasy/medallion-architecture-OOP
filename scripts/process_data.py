"""Executable file to create the gold tables from the silver files."""

from src.data_pipeline.gold_layer.runner_performances import RunnerPerformances
from src.data_pipeline.silver_layer.silver_runs import SilverRuns
from src.data_pipeline.silver_layer.silver_users import SilverUsers


def process_gold_tables() -> None:
    """Process the gold tables."""
    runner_performances = RunnerPerformances().build()
    runner_performances.to_csv("data/gold/runner_performances.csv", index=False)


def process_silver_tables() -> None:
    """Process the silver tables."""
    filtered_runs = SilverRuns().build()
    filtered_users = SilverUsers().build()
    filtered_runs.to_csv("data/silver/runs.csv", index=False)
    filtered_users.to_csv("data/silver/users.csv", index=False)


if __name__ == "__main__":
    process_silver_tables()
    process_gold_tables()
