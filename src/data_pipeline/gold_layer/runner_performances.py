"""File to compute the gold layer table : runner_performances."""

from src.constants import (
    FIVEK_DISTANCE,
    HALF_MARATHON_DISTANCE,
    MARATHON_DISTANCE,
    SILVER_RUNS_COLUMNS,
    TENK_DISTANCE,
)
from src.data_pipeline.abstract_processed_table import ProcessedTable, SourceTable


class RunnerPerformances(ProcessedTable):
    """
    Gold layer table containing the performances of each runner on different distances.
    """

    name = "runner_performances"
    source_tables = [
        SourceTable(name="runs", path_to_table="data/silver/runs.csv"),
        SourceTable(name="users", path_to_table="data/silver/users.csv"),
    ]
    processed_table_schema = {
        "user_id": "int",
        "last_duration_on_marathon": "float64",
        "last_duration_on_half_marathon": "float64",
        "last_duration_on_10k": "float64",
        "last_duration_on_5k": "float64",
    }

    def process_data(self, data_sources):
        """Process the 'runner_performances' data.
        This function computes the average speed and temperature for each user.
        """
        runs = data_sources["runs"]
        users = data_sources["users"]
        last_durations_on_marathon = (
            runs[runs["distance"] == MARATHON_DISTANCE]
            .groupby("user_id")["duration"]
            .last()
            .rename("last_duration_on_marathon")
        )
        last_durations_on_half_marathon = (
            runs[runs["distance"] == HALF_MARATHON_DISTANCE]
            .groupby("user_id")["duration"]
            .last()
            .rename("last_duration_on_half_marathon")
        )
        last_durations_on_10k = (
            runs[runs["distance"] == TENK_DISTANCE]
            .groupby("user_id")["duration"]
            .last()
            .rename("last_duration_on_10k")
        )
        last_durations_on_5k = (
            runs[runs["distance"] == FIVEK_DISTANCE]
            .groupby("user_id")["duration"]
            .last()
            .rename("last_duration_on_5k")
        )
        return (
            users.merge(last_durations_on_half_marathon, on="user_id", how="left")
            .merge(last_durations_on_marathon, on="user_id", how="left")
            .merge(last_durations_on_10k, on="user_id", how="left")
            .merge(last_durations_on_5k, on="user_id", how="left")
        )

    def validate_data_sources_quality(self, data_sources):
        """Validate the quality of the 'runner_performances' data.

        This function checks that the 'runs' and 'users' data contain the expected columns.
        """
        expected_columns_runs = SILVER_RUNS_COLUMNS
        assert all(
            column in data_sources["runs"].columns for column in expected_columns_runs
        ), f"Columns in 'runs' data do not match expected columns: {expected_columns_runs}"

        expected_columns_users = ["user_id"]
        assert all(
            column in data_sources["users"].columns for column in expected_columns_users
        ), f"Columns in 'users' data do not match expected columns: {expected_columns_users}"

    def validate_processed_data_quality(self, processed_data):
        expected_columns = [
            "user_id",
            "last_duration_on_half_marathon",
            "last_duration_on_marathon",
            "last_duration_on_10k",
            "last_duration_on_5k",
        ]
        assert all(
            column in processed_data.columns for column in expected_columns
        ), f"""
        Columns in 'runner_performances' data do not match expected columns: {expected_columns}"""
        assert processed_data["user_id"].nunique() == len(
            processed_data
        ), "Duplicate user_id found"
