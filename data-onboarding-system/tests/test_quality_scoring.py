"""Tests for the 5-component weighted quality scoring model."""

import pandas as pd
import numpy as np

from app.analysis.quality_checks import QualityChecker
from app.config import AnalysisConfig


def _make_checker():
    return QualityChecker(connector=object(), config=AnalysisConfig())


def test_quality_score_returns_contract_shape():
    """Output must include overall_score, components, weights, severity_counts."""
    checker = _make_checker()
    profile = {
        "total_rows": 100,
        "columns": {
            "id": {"null_percent": 0, "unique_percent": 100, "data_type": "integer"},
            "name": {"null_percent": 5, "unique_percent": 80, "data_type": "text"},
        },
    }
    df = pd.DataFrame({"id": range(100), "name": [f"n{i}" for i in range(100)]})
    result = checker.check_table_quality("t", profile, df)

    assert "overall_score" in result
    assert "components" in result
    assert "weights" in result
    assert "severity_counts" in result
    assert 0 <= result["overall_score"] <= 100


def test_quality_score_has_backward_compat_alias():
    """quality_score and severity_summary should still exist."""
    checker = _make_checker()
    profile = {
        "total_rows": 10,
        "columns": {"id": {"null_percent": 0, "unique_percent": 100, "data_type": "integer"}},
    }
    df = pd.DataFrame({"id": range(10)})
    result = checker.check_table_quality("t", profile, df)

    assert result["quality_score"] == result["overall_score"]
    assert "severity_summary" in result


def test_perfect_data_scores_high():
    checker = _make_checker()
    profile = {
        "total_rows": 50,
        "columns": {
            "id": {"null_percent": 0, "unique_percent": 100, "data_type": "integer"},
            "val": {"null_percent": 0, "unique_percent": 90, "data_type": "numeric"},
        },
    }
    df = pd.DataFrame({"id": range(50), "val": range(50)})
    result = checker.check_table_quality("t", profile, df)

    assert result["overall_score"] >= 80


def test_all_null_data_scores_low():
    checker = _make_checker()
    profile = {
        "total_rows": 50,
        "columns": {
            "id": {"null_percent": 100, "unique_percent": 0, "data_type": "integer"},
            "val": {"null_percent": 100, "unique_percent": 0, "data_type": "text"},
        },
    }
    df = pd.DataFrame({"id": [None]*50, "val": [None]*50})
    result = checker.check_table_quality("t", profile, df)

    assert result["overall_score"] <= 50
    assert sum(result["severity_counts"].values()) > 0  # Has some severity issues


def test_weights_sum_to_100():
    checker = _make_checker()
    profile = {
        "total_rows": 10,
        "columns": {"x": {"null_percent": 0, "unique_percent": 50, "data_type": "text"}},
    }
    df = pd.DataFrame({"x": list("abcde") * 2})
    result = checker.check_table_quality("t", profile, df)

    total_weight = sum(result["weights"].values())
    assert total_weight == 100
