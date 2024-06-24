"""File to compute the silver layer table : silver_runs."""

from typing import Dict

import pandas as pd

from src.constants import SILVER_RUNS_COLUMNS
from src.data_pipeline.abstract_processed_table import ProcessedTable, SourceTable


class SilverRuns(ProcessedTable):
    """
    Silver layer table containing the filtered 'runs' data.
    """

    name = "silver_runs"
    source_tables = [SourceTable(name="runs", path_to_table="data/bronze/runs.csv")]
    processed_table_schema = {
        "run_id": "int",
        "date": "datetime64",
        "distance": "float64",
        "duration": "float64",
        "temperature": "float64",
        "user_id": "int",
    }

    def process_data(self, data_sources: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Process the 'runs' data.

        This function performs the following operations:
        1. Converts the 'date' column to a consistent datetime format.
        2. Converts all values in the 'temperature' column to degrees Celsius,
        regardless of the original unit (Celsius or Fahrenheit).
        3. Filters out any runs that lasted less than 5 minutes (300 seconds).
        4. Removes the 'location' column from the dataset.
        """
        return (
            data_sources["runs"]
            .assign(
                date=pd.to_datetime(
                    data_sources["runs"]["date"], errors="coerce", format="mixed"
                ),
                temperature=data_sources["runs"]["temperature"].apply(
                    self._convert_temperature
                ),
            )
            .query("duration >= 5*60")
            .drop(columns=["location"])
        )

    def _convert_temperature(self, temperature: str) -> float:
        """
        Convert the temperature to degrees Celsius.

        This function converts the temperature to degrees Celsius,
        regardless of the original unit (Celsius or Fahrenheit).
        """
        if temperature.endswith("°F"):
            return (float(temperature[:-2]) - 32) * 5 / 9
        return float(temperature[:-2])

    def validate_data_sources_quality(
        self, data_sources: Dict[str, pd.DataFrame]
    ) -> None:
        """
        Validate the quality of the 'runs' data.

        This function checks that the 'runs' data contains the expected columns.
        """
        expected_columns = SILVER_RUNS_COLUMNS + [
            "location",
            "user_id",
        ]
        assert all(
            column in data_sources["runs"].columns for column in expected_columns
        ), f"Missing columns in 'runs' data. Expected columns: {expected_columns}"
        # Check that all distances are positive
        assert (
            data_sources["runs"]["distance"] >= 0
        ).all(), "There are negative distances in the 'runs' data."
        # Check that all durations are positive
        assert (
            data_sources["runs"]["duration"] >= 0
        ).all(), "There are negative durations in the 'runs' data."
        # Check that all the speeds are below 50 km/h
        assert (
            data_sources["runs"]["distance"] / (data_sources["runs"]["duration"] * 3600)
            <= 50
        ).all(), "There are speeds above 50 km/h in the 'runs' data."
        # Check that all temperatures have the expected format : end with '°C' or '°F'
        assert (
            data_sources["runs"]["temperature"].str.endswith("°C")
            | data_sources["runs"]["temperature"].str.endswith("°F")
        ).all(), (
            "There are temperatures without the expected format in the 'runs' data."
        )

    def validate_processed_data_quality(self, processed_data: pd.DataFrame) -> None:
        """
        Validate the quality of the processed 'runs' data.

        This function checks that the processed 'runs' data contains the expected columns.
        """
        expected_columns = SILVER_RUNS_COLUMNS
        assert all(
            column in processed_data.columns for column in expected_columns
        ), f"Missing columns in processed 'runs' data. Expected columns: {expected_columns}"
        assert (
            processed_data["temperature"].max() <= 60
        ), "There are temperature values above 60°C."
        # Check that all dates are in the expected format
        assert (
            processed_data["date"].notnull().all()
        ), "There are missing dates in the processed 'runs' data."
