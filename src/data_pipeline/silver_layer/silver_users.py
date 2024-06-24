"""File to compute the silver layer table : silver_users."""

from typing import Dict

import pandas as pd

from src.constants import MARATHON_DISTANCE, SILVER_RUNS_COLUMNS
from src.data_pipeline.abstract_processed_table import ProcessedTable, SourceTable


class SilverUsers(ProcessedTable):
    """
    Silver layer table containing the filtered 'users' data.
    """

    name = "silver_users"
    source_tables = [SourceTable(name="runs", path_to_table="data/bronze/runs.csv")]
    processed_table_schema = {"user_id": "int"}

    def process_data(self, data_sources: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Process the 'users' data.

        This function filters out any users who have not recorded a marathon.
        """
        runs = data_sources["runs"]
        return runs[runs["distance"] == MARATHON_DISTANCE].drop_duplicates(
            subset="user_id"
        )[["user_id"]]

    def validate_data_sources_quality(
        self, data_sources: Dict[str, pd.DataFrame]
    ) -> None:
        """
        Validate the quality of the 'users' data.

        This function checks that the 'runs' data contains the expected columns.
        """
        expected_columns = SILVER_RUNS_COLUMNS
        assert all(
            column in data_sources["runs"].columns for column in expected_columns
        ), f"Columns in 'runs' data do not match expected columns: {expected_columns}"
        # Check that there is at least one user who has recorded a marathon
        assert any(
            data_sources["runs"]["distance"] == MARATHON_DISTANCE
        ), "No users have recorded a marathon"

    def validate_processed_data_quality(self, processed_data: pd.DataFrame) -> None:
        expected_columns = ["user_id"]
        assert all(
            column in processed_data.columns for column in expected_columns
        ), f"Columns in 'users' data do not match expected columns: {expected_columns}"
        assert (
            processed_data["user_id"].nunique() > 0
        ), "No users have recorded a marathon"
        assert processed_data["user_id"].nunique() == len(
            processed_data
        ), "Duplicate user_id found"
