"""Intelligent sampling strategy for data extraction."""

from typing import Dict, Any, Optional
import pandas as pd
from loguru import logger

from app.config import SamplingConfig
from app.connectors.postgres import PostgreSQLConnector


class SamplingStrategy:
    """Determine and execute sampling strategies per table."""
    
    def __init__(self, connector: PostgreSQLConnector, config: SamplingConfig):
        self.connector = connector
        self.config = config
    
    def determine_strategy(self, table: str, row_count: int, schema: str = "public") -> Dict[str, Any]:
        """
        Determine sampling strategy based on table size.
        
        Returns:
            Dict with strategy details: method, sample_rate, expected_rows
        """
        if not self.config.enabled or row_count <= self.config.small_table_threshold:
            return {
                "method": "full",
                "sample_rate": 1.0,
                "expected_rows": row_count,
                "reason": "Small table - full scan"
            }
        
        elif row_count <= 10000000:  # Medium tables
            sample_rate = self.config.medium_sample_rate
            expected = min(int(row_count * sample_rate), self.config.max_sample_size)
            return {
                "method": "sample",
                "sample_rate": sample_rate,
                "expected_rows": expected,
                "reason": "Medium table - 10% sample"
            }
        
        else:  # Large tables
            sample_rate = self.config.large_sample_rate
            expected = min(int(row_count * sample_rate), self.config.max_sample_size)
            return {
                "method": "sample",
                "sample_rate": sample_rate,
                "expected_rows": expected,
                "reason": "Large table - 5% sample"
            }
    
    def extract_sample(
        self, 
        table: str, 
        strategy: Dict[str, Any],
        schema: str = "public",
        columns: Optional[list] = None
    ) -> pd.DataFrame:
        """
        Extract data according to sampling strategy.
        
        Args:
            table: Table name
            strategy: Strategy dict from determine_strategy()
            schema: Database schema
            columns: Optional list of columns to select (None = all)
            
        Returns:
            DataFrame with sampled data
        """
        logger.debug(f"Extracting sample from {table}: {strategy['reason']}")
        
        try:
            if strategy["method"] == "full":
                query = f'SELECT * FROM "{schema}"."{table}"'
                return self.connector.execute_query(query)
            
            else:  # sample
                return self.connector.sample_table(
                    table=table,
                    sample_rate=strategy["sample_rate"],
                    max_rows=self.config.max_sample_size,
                    schema=schema
                )
        
        except Exception as e:
            logger.error(f"Failed to extract sample from {table}: {e}")
            return pd.DataFrame()
    
    def stratified_sample(
        self,
        table: str,
        date_column: str,
        n_strata: int = 10,
        schema: str = "public"
    ) -> pd.DataFrame:
        """
        Extract stratified sample across time periods.
        
        Useful for time-series data to ensure temporal coverage.
        """
        logger.debug(f"Extracting stratified sample from {table} by {date_column}")
        
        # Get date range
        query = f"""
            SELECT 
                MIN("{date_column}") as min_date,
                MAX("{date_column}") as max_date
            FROM "{schema}"."{table}"
            WHERE "{date_column}" IS NOT NULL
        """
        date_range = self.connector.execute_query(query)
        
        if date_range.empty:
            logger.warning(f"No valid dates in {date_column}, falling back to random sample")
            return self.connector.sample_table(table, schema=schema)
        
        # Sample from each stratum
        # This is a simplified version - production would use ntile() or similar
        query = f"""
            SELECT * FROM "{schema}"."{table}"
            TABLESAMPLE SYSTEM (10)
            WHERE "{date_column}" IS NOT NULL
            LIMIT {self.config.max_sample_size}
        """
        
        return self.connector.execute_query(query)
