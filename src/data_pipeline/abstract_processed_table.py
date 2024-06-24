"""Abstract class for processed tables."""

from abc import ABC, abstractmethod
from typing import Dict, List

import pandas as pd
from loguru import logger


# pylint: disable=too-few-public-methods
class SourceTable(ABC):
    """Abstract class for source tables."""

    name: str = ""
    path_to_table: str = ""

    def __init__(self, name: str, path_to_table: str) -> None:
        self.name = name
        self.path_to_table = path_to_table

    def load_data(self) -> pd.DataFrame:
        """Load the data from the source table."""
        return pd.read_csv(self.path_to_table)


class ProcessedTable(ABC):
    """Abstract class for processed tables."""

    name: str = ""
    source_tables: List[SourceTable] = []
    processed_table_schema: Dict[str, str] = {}

    def ingest_source_tables(
        self,
        *,
        source_tables: List[SourceTable],
    ) -> Dict[str, pd.DataFrame]:
        """Ingest data from the data source."""
        return {
            source_table.name: source_table.load_data()
            for source_table in source_tables
        }

    @abstractmethod
    def process_data(self, data_sources: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Process the data: cleaning, reformating, renaming, computation."""

    @abstractmethod
    def validate_data_sources_quality(
        self, data_sources: Dict[str, pd.DataFrame]
    ) -> None:
        """Validate the quality of the data sources."""

    @abstractmethod
    def validate_processed_data_quality(self, processed_data: pd.DataFrame) -> None:
        """Validate the quality of the processed data."""

    def build(self) -> pd.DataFrame:
        """Build the processed table."""
        logger.info(f"{self.name} - Ingesting data sources...")
        data_sources = self.ingest_source_tables(source_tables=self.source_tables)
        self.validate_data_sources_quality(data_sources)
        logger.success(f"{self.name} - Data sources ingested and validated!")

        logger.info(f"{self.name} - Processing data...")
        processed_data = self.process_data(data_sources)
        self.validate_processed_data_quality(processed_data)
        logger.success(f"{self.name} - Data processed and validated!")
        return processed_data
