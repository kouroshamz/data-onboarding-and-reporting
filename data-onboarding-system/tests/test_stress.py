"""Large-dataset stress tests — synthetic data to validate memory & performance."""

import time

import numpy as np
import pandas as pd
import pytest

from app.analysis.profiling import DataProfiler
from app.analysis.quality_checks import QualityChecker
from app.analysis.pii_scan import PIIScanner
from app.analysis.structural_overview import compute_structural_overview
from app.analysis.readiness_score import compute_readiness_score
from app.analysis.column_classifier import classify_columns


def _generate_wide_dataframe(nrows: int = 50_000, ncols: int = 50) -> pd.DataFrame:
    """Synthetic wide table with mixed dtypes."""
    np.random.seed(42)
    data = {}
    for i in range(ncols):
        mod = i % 5
        if mod == 0:
            data[f"int_col_{i}"] = np.random.randint(0, 10000, nrows)
        elif mod == 1:
            data[f"float_col_{i}"] = np.random.uniform(0, 1000, nrows)
        elif mod == 2:
            data[f"cat_col_{i}"] = np.random.choice(["A", "B", "C", "D"], nrows)
        elif mod == 3:
            data[f"email_col_{i}"] = [f"user{j}@example.com" for j in range(nrows)]
        else:
            data[f"text_col_{i}"] = [f"text_{j}" for j in range(nrows)]
    # Inject 5% nulls into every third column
    df = pd.DataFrame(data)
    for col in df.columns[::3]:
        mask = np.random.random(nrows) < 0.05
        df.loc[mask, col] = np.nan
    return df


@pytest.mark.slow
class TestLargeDatasetStress:

    @pytest.fixture(scope="class")
    def large_df(self):
        return _generate_wide_dataframe(50_000, 50)

    def test_profiling_completes_under_30s(self, large_df):
        """Profiler should handle 50K x 50 within 30 seconds."""
        profiler = DataProfiler.__new__(DataProfiler)
        profiler.config = type("C", (), {
            "top_values_limit": 10, "outlier_method": "iqr", "outlier_threshold": 3.0,
        })()
        t0 = time.time()
        result = profiler.profile_table("stress_test", large_df)
        elapsed = time.time() - t0
        assert elapsed < 30, f"Profiling took {elapsed:.1f}s — too slow"
        assert len(result["columns"]) == 50

    def test_quality_check_completes(self, large_df):
        """Quality checker should handle large data without errors."""
        profiler = DataProfiler.__new__(DataProfiler)
        profiler.config = type("C", (), {
            "top_values_limit": 10, "outlier_method": "iqr", "outlier_threshold": 3.0,
        })()
        profile = profiler.profile_table("stress_test", large_df)
        cfg = type("C", (), {
            "max_null_percent": 50, "min_freshness_days": 90,
        })()
        checker = QualityChecker(connector=None, config=cfg)
        quality = checker.check_table_quality("stress_test", profile, large_df)
        assert "overall_score" in quality or "quality_score" in quality

    def test_pii_scanner_handles_wide_table(self, large_df):
        columns = [{"column_name": c, "data_type": "text"} for c in large_df.columns]
        scanner = PIIScanner()
        result = scanner.scan_table("stress_test", columns, large_df)
        assert "has_pii" in result
        # Should flag email columns
        assert result["has_pii"] is True

    def test_structural_overview_wide(self, large_df):
        """Structural overview should work on wide tables."""
        profiler = DataProfiler.__new__(DataProfiler)
        profiler.config = type("C", (), {
            "top_values_limit": 10, "outlier_method": "iqr", "outlier_threshold": 3.0,
        })()
        profile = profiler.profile_table("stress_test", large_df)
        schema = {
            "table_count": 1,
            "tables": {
                "stress_test": {
                    "row_count": len(large_df),
                    "columns": [{"name": c} for c in large_df.columns],
                }
            },
        }
        overview = compute_structural_overview(
            schema, {"stress_test": profile}, {"stress_test": large_df}, [],
        )
        assert overview["total_rows"] == 50_000
        assert overview["total_columns"] == 50

    def test_column_classifier_handles_50_cols(self, large_df):
        profiler = DataProfiler.__new__(DataProfiler)
        profiler.config = type("C", (), {
            "top_values_limit": 10, "outlier_method": "iqr", "outlier_threshold": 3.0,
        })()
        profile = profiler.profile_table("stress_test", large_df)
        schema = {
            "table_count": 1,
            "tables": {
                "stress_test": {
                    "row_count": len(large_df),
                    "columns": [{"name": c} for c in large_df.columns],
                }
            },
        }
        result = classify_columns({"stress_test": profile}, schema)
        assert "summary" in result
