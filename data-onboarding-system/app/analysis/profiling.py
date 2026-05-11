"""Data profiling engine using SQL pushdown and pandas analysis."""

from typing import Dict, Any, List
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from loguru import logger

from app.connectors.postgres import PostgreSQLConnector
from app.config import AnalysisConfig


class DataProfiler:
    """Profile table data with efficient SQL and sampling."""
    
    def __init__(self, connector: PostgreSQLConnector, config: AnalysisConfig):
        self.connector = connector
        self.config = config
    
    def profile_table(
        self, 
        table: str, 
        sample_data: pd.DataFrame,
        schema: str = "public"
    ) -> Dict[str, Any]:
        """
        Generate comprehensive profile for a table.
        
        Args:
            table: Table name
            sample_data: Sample data DataFrame
            schema: Database schema
            
        Returns:
            Profile dictionary with column-level statistics
        """
        logger.info(f"Profiling table: {table}")
        
        if sample_data.empty:
            return {"error": "No data available for profiling"}
        
        profile = {
            "table_name": table,
            "profiled_at": datetime.now(timezone.utc).isoformat(),
            "sample_size": len(sample_data),
            "columns": {}
        }
        
        # Profile each column
        for col in sample_data.columns:
            profile["columns"][col] = self._profile_column(
                col, sample_data[col], table, schema
            )
        
        # Add table-level statistics
        profile["completeness_score"] = self._calculate_completeness(profile["columns"])
        profile["data_types"] = self._summarize_data_types(profile["columns"])
        
        logger.info(f"Profiled {len(profile['columns'])} columns from {table}")
        return profile
    
    def _profile_column(
        self, 
        col_name: str, 
        col_data: pd.Series,
        table: str,
        schema: str
    ) -> Dict[str, Any]:
        """Profile a single column."""
        col_profile = {
            "name": col_name,
            "dtype": str(col_data.dtype),
            "total_count": len(col_data),
            "null_count": int(col_data.isna().sum()),
            "null_percent": float(col_data.isna().mean() * 100),
            "unique_count": int(col_data.nunique()),
            "unique_percent": float(col_data.nunique() / len(col_data) * 100) if len(col_data) > 0 else 0
        }
        
        # Type-specific profiling
        if pd.api.types.is_bool_dtype(col_data):
            col_profile.update(self._profile_boolean(col_data))

        elif pd.api.types.is_numeric_dtype(col_data):
            col_profile.update(self._profile_numeric(col_data))
        
        elif pd.api.types.is_string_dtype(col_data) or pd.api.types.is_object_dtype(col_data):
            col_profile.update(self._profile_string(col_data))
        
        elif pd.api.types.is_datetime64_any_dtype(col_data):
            col_profile.update(self._profile_datetime(col_data))
        
        # Top values (for all types)
        col_profile["top_values"] = self._get_top_values(col_data)
        
        return col_profile
    
    def _profile_numeric(self, data: pd.Series) -> Dict[str, Any]:
        """Profile numeric column."""
        clean_data = data.dropna()
        # Guard against boolean-like data that slipped through
        if clean_data.dtype == bool or clean_data.dtype == "bool":
            return self._profile_boolean(data)

        if len(clean_data) == 0:
            return {"type_category": "numeric", "statistics": {}}
        
        stats = {
            "type_category": "numeric",
            "statistics": {
                "min": float(clean_data.min()),
                "max": float(clean_data.max()),
                "mean": float(clean_data.mean()),
                "median": float(clean_data.median()),
                "std": float(clean_data.std()) if len(clean_data) > 1 else 0,
                "q25": float(clean_data.quantile(0.25)),
                "q75": float(clean_data.quantile(0.75)),
            }
        }
        
        # Outlier detection
        if self.config.outlier_method == "iqr":
            stats["outliers"] = self._detect_outliers_iqr(clean_data)
        else:
            stats["outliers"] = self._detect_outliers_zscore(clean_data)
        
        return stats
    
    def _profile_boolean(self, data: pd.Series) -> Dict[str, Any]:
        """Profile boolean column."""
        clean = data.dropna()
        true_count = int(clean.sum()) if len(clean) else 0
        false_count = len(clean) - true_count
        return {
            "type_category": "boolean",
            "statistics": {
                "true_count": true_count,
                "false_count": false_count,
                "true_percent": round(true_count / max(len(clean), 1) * 100, 1),
            },
        }

    def _profile_string(self, data: pd.Series) -> Dict[str, Any]:
        """Profile string/text column."""
        clean_data = data.dropna().astype(str)
        
        if len(clean_data) == 0:
            return {"type_category": "string", "patterns": {}}
        
        lengths = clean_data.str.len()
        
        return {
            "type_category": "string",
            "patterns": {
                "min_length": int(lengths.min()),
                "max_length": int(lengths.max()),
                "avg_length": float(lengths.mean()),
                "empty_strings": int((clean_data == "").sum()),
                "has_email_pattern": bool(clean_data.str.contains(r'@', regex=True).any()),
                "has_phone_pattern": bool(clean_data.str.contains(r'\d{3}[-\s]?\d{3}[-\s]?\d{4}', regex=True).any()),
                "has_url_pattern": bool(clean_data.str.contains(r'http[s]?://', regex=True).any()),
            }
        }
    
    def _profile_datetime(self, data: pd.Series) -> Dict[str, Any]:
        """Profile datetime column."""
        clean_data = data.dropna()
        
        if len(clean_data) == 0:
            return {"type_category": "datetime", "temporal": {}}
        
        return {
            "type_category": "datetime",
            "temporal": {
                "min_date": str(clean_data.min()),
                "max_date": str(clean_data.max()),
                "range_days": int((clean_data.max() - clean_data.min()).days),
                "most_recent": str(clean_data.max()),
                "oldest": str(clean_data.min()),
            }
        }
    
    def _get_top_values(self, data: pd.Series) -> List[Dict[str, Any]]:
        """Get top N most frequent values."""
        value_counts = data.value_counts().head(self.config.top_values_limit)
        
        return [
            {
                "value": str(val) if not pd.isna(val) else None,
                "count": int(count),
                "percent": float(count / len(data) * 100)
            }
            for val, count in value_counts.items()
        ]
    
    def _detect_outliers_iqr(self, data: pd.Series) -> Dict[str, Any]:
        """Detect outliers using IQR method."""
        q1 = data.quantile(0.25)
        q3 = data.quantile(0.75)
        iqr = q3 - q1
        
        lower_bound = q1 - (self.config.outlier_threshold * iqr)
        upper_bound = q3 + (self.config.outlier_threshold * iqr)
        
        outliers = data[(data < lower_bound) | (data > upper_bound)]
        
        return {
            "method": "iqr",
            "count": int(len(outliers)),
            "percent": float(len(outliers) / len(data) * 100),
            "lower_bound": float(lower_bound),
            "upper_bound": float(upper_bound),
        }
    
    def _detect_outliers_zscore(self, data: pd.Series) -> Dict[str, Any]:
        """Detect outliers using Z-score method."""
        mean = data.mean()
        std = data.std()
        
        if std == 0:
            return {"method": "zscore", "count": 0, "percent": 0}
        
        z_scores = np.abs((data - mean) / std)
        outliers = data[z_scores > self.config.outlier_threshold]
        
        return {
            "method": "zscore",
            "count": int(len(outliers)),
            "percent": float(len(outliers) / len(data) * 100),
            "threshold": self.config.outlier_threshold,
        }
    
    def _calculate_completeness(self, columns: Dict[str, Any]) -> float:
        """Calculate overall data completeness score (0-100)."""
        if not columns:
            return 0.0
        
        null_percents = [col["null_percent"] for col in columns.values() if "null_percent" in col]
        if not null_percents:
            return 100.0
        
        avg_completeness = 100 - np.mean(null_percents)
        return round(float(avg_completeness), 2)
    
    def _summarize_data_types(self, columns: Dict[str, Any]) -> Dict[str, int]:
        """Summarize data types across columns."""
        type_counts = {}
        for col in columns.values():
            dtype = col.get("type_category", col.get("dtype", "unknown"))
            type_counts[dtype] = type_counts.get(dtype, 0) + 1
        
        return type_counts
