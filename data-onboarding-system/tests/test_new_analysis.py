"""Tests for new Phase 7 analysis modules:
  - structural_overview
  - gdpr_assessment
  - column_classifier
  - interesting_columns
  - missing_strategy
  - readiness_score
"""

import pytest
import pandas as pd
import numpy as np


# ── Structural Overview ──────────────────────────────────────────────────────

class TestStructuralOverview:
    def _sample_profile(self):
        return {
            "people": {
                "sample_size": 100,
                "columns": {
                    "id": {"null_percent": 0, "unique_count": 100, "unique_percent": 100, "type_category": "numeric"},
                    "name": {"null_percent": 5, "unique_count": 80, "unique_percent": 80, "type_category": "string"},
                    "age": {"null_percent": 0, "unique_count": 50, "unique_percent": 50, "type_category": "numeric"},
                    "constant": {"null_percent": 0, "unique_count": 1, "unique_percent": 1, "type_category": "string"},
                    "empty": {"null_percent": 100, "unique_count": 0, "unique_percent": 0, "type_category": "string"},
                },
            }
        }

    def _sample_frames(self):
        df = pd.DataFrame({
            "id": range(100),
            "name": [f"person_{i}" for i in range(100)],
            "age": list(range(50)) * 2,
            "constant": ["A"] * 100,
            "empty": [None] * 100,
        })
        # Add some duplicates
        df = pd.concat([df, df.iloc[:5]], ignore_index=True)
        return {"people": df}

    def test_basic_overview(self):
        from app.analysis.structural_overview import compute_structural_overview
        result = compute_structural_overview(
            {}, self._sample_profile(), self._sample_frames()
        )
        assert result["total_rows"] == 100
        assert result["total_columns"] == 5
        assert result["total_duplicate_rows"] == 5
        assert result["columns_fully_null"] == 1
        assert len(result["constant_columns"]) == 1
        assert result["estimated_memory_bytes"] > 0

    def test_suspicious_ids(self):
        from app.analysis.structural_overview import compute_structural_overview
        result = compute_structural_overview(
            {}, self._sample_profile(), self._sample_frames()
        )
        assert any("id" in s.lower() for s in result["suspicious_id_columns"])

    def test_dtype_distribution(self):
        from app.analysis.structural_overview import compute_structural_overview
        result = compute_structural_overview(
            {}, self._sample_profile(), self._sample_frames()
        )
        assert "numeric" in result["dtype_distribution"]
        assert "string" in result["dtype_distribution"]


# ── GDPR Assessment ──────────────────────────────────────────────────────────

class TestGDPRAssessment:
    def test_no_pii(self):
        from app.analysis.gdpr_assessment import compute_gdpr_assessment
        result = compute_gdpr_assessment({}, {})
        assert result["total_pii_findings"] == 0
        assert result["overall_risk"] == "low"

    def test_with_pii(self):
        from app.analysis.gdpr_assessment import compute_gdpr_assessment
        pii = {
            "users": {
                "findings": [
                    {"column": "email", "pii_type": "email", "confidence": 0.9},
                    {"column": "ssn", "pii_type": "ssn", "confidence": 0.95},
                ]
            }
        }
        result = compute_gdpr_assessment(pii, {})
        assert result["total_pii_findings"] == 2
        assert result["overall_risk"] == "critical"
        assert "contact_data" in result["gdpr_categories"]
        assert "national_id" in result["gdpr_categories"]

    def test_special_category(self):
        from app.analysis.gdpr_assessment import compute_gdpr_assessment
        pii = {
            "patients": {
                "findings": [
                    {"column": "diagnosis", "pii_type": "health", "confidence": 0.85},
                ]
            }
        }
        result = compute_gdpr_assessment(pii, {})
        assert result["has_special_category_data"] is True

    def test_recommendations(self):
        from app.analysis.gdpr_assessment import compute_gdpr_assessment
        pii = {
            "users": {"findings": [{"column": "email", "pii_type": "email", "confidence": 0.9}]}
        }
        result = compute_gdpr_assessment(pii, {})
        recs = result["recommendations"]
        assert len(recs) >= 3  # lawful basis, minimisation, retention at minimum
        areas = [r["area"] for r in recs]
        assert any("Lawful Basis" in a for a in areas)


# ── Column Classifier ────────────────────────────────────────────────────────

class TestColumnClassifier:
    def test_revenue_column(self):
        from app.analysis.column_classifier import classify_columns
        profile = {
            "orders": {
                "columns": {
                    "total_price": {"type_category": "numeric", "dtype": "float64",
                                    "stats": {"min": 0}, "null_percent": 0,
                                    "unique_count": 80, "unique_percent": 80},
                }
            }
        }
        result = classify_columns(profile)
        assert "revenue" in result["by_category"]

    def test_timestamp_column(self):
        from app.analysis.column_classifier import classify_columns
        profile = {
            "events": {
                "columns": {
                    "created_date": {"type_category": "datetime", "dtype": "datetime64",
                                     "null_percent": 0, "unique_count": 100, "unique_percent": 100},
                }
            }
        }
        result = classify_columns(profile)
        assert "timestamp" in result["by_category"]

    def test_status_column(self):
        from app.analysis.column_classifier import classify_columns
        profile = {
            "orders": {
                "columns": {
                    "order_status": {"type_category": "categorical", "dtype": "object",
                                     "null_percent": 0, "unique_count": 5, "unique_percent": 5},
                }
            }
        }
        result = classify_columns(profile)
        assert "status_lifecycle" in result["by_category"]

    def test_no_match(self):
        from app.analysis.column_classifier import classify_columns
        profile = {
            "data": {
                "columns": {
                    "xyz_metric": {"type_category": "numeric", "dtype": "float64",
                                   "null_percent": 0, "unique_count": 100, "unique_percent": 100,
                                   "stats": {"min": -5}},
                }
            }
        }
        result = classify_columns(profile)
        assert result["summary"]["total_classified"] == 0


# ── Interesting Columns ──────────────────────────────────────────────────────

class TestInterestingColumns:
    def test_high_variance(self):
        from app.analysis.interesting_columns import detect_interesting_columns
        profile = {
            "data": {
                "columns": {
                    "wild_col": {
                        "type_category": "numeric",
                        "stats": {"mean": 10, "std": 50},
                        "null_percent": 0, "unique_count": 100, "unique_percent": 100,
                    },
                }
            }
        }
        df = pd.DataFrame({"wild_col": np.random.exponential(10, 100)})
        result = detect_interesting_columns(profile, {"data": df})
        assert result["count"] >= 1
        types = [r["type"] for col in result["interesting_columns"] for r in col["reasons"]]
        assert "high_variance" in types

    def test_correlations(self):
        from app.analysis.interesting_columns import detect_interesting_columns
        df = pd.DataFrame({
            "a": range(100),
            "b": [x * 2 + np.random.normal(0, 0.1) for x in range(100)],
            "c": np.random.random(100),
        })
        profile = {
            "data": {
                "columns": {
                    "a": {"type_category": "numeric", "stats": {"mean": 50, "std": 29}, "null_percent": 0, "unique_count": 100, "unique_percent": 100},
                    "b": {"type_category": "numeric", "stats": {"mean": 100, "std": 58}, "null_percent": 0, "unique_count": 100, "unique_percent": 100},
                    "c": {"type_category": "numeric", "stats": {"mean": 0.5, "std": 0.29}, "null_percent": 0, "unique_count": 100, "unique_percent": 100},
                }
            }
        }
        result = detect_interesting_columns(profile, {"data": df})
        corrs = result["correlations"].get("data", [])
        assert len(corrs) >= 1
        assert any(c["col_a"] == "a" and c["col_b"] == "b" for c in corrs)


# ── Missing Data Strategy ────────────────────────────────────────────────────

class TestMissingStrategy:
    def test_no_nulls(self):
        from app.analysis.missing_strategy import compute_missing_strategy
        profile = {
            "data": {"columns": {"col1": {"null_percent": 0, "type_category": "numeric"}}}
        }
        result = compute_missing_strategy(profile)
        assert result["summary"]["total_columns_with_nulls"] == 0

    def test_high_null_drop(self):
        from app.analysis.missing_strategy import compute_missing_strategy
        profile = {
            "data": {
                "sample_size": 100,
                "columns": {
                    "mostly_empty": {"null_percent": 90, "type_category": "string",
                                     "unique_count": 5, "unique_percent": 5},
                }
            }
        }
        result = compute_missing_strategy(profile)
        strategies = result["strategies"]
        assert len(strategies) == 1
        assert strategies[0]["treatment_type"] == "drop"

    def test_numeric_impute(self):
        from app.analysis.missing_strategy import compute_missing_strategy
        profile = {
            "data": {
                "sample_size": 100,
                "columns": {
                    "metric": {"null_percent": 15, "type_category": "numeric",
                               "unique_count": 80, "unique_percent": 80,
                               "stats": {"skewness": 0.2}},
                }
            }
        }
        result = compute_missing_strategy(profile)
        assert result["strategies"][0]["treatment_type"] == "impute"

    def test_important_column_not_dropped(self):
        from app.analysis.missing_strategy import compute_missing_strategy
        profile = {
            "data": {
                "sample_size": 100,
                "columns": {
                    "revenue": {"null_percent": 85, "type_category": "numeric",
                                "unique_count": 50, "unique_percent": 50,
                                "stats": {}},
                }
            }
        }
        classifications = {
            "by_category": {"revenue": [{"table": "data", "column": "revenue", "confidence": 0.8}]},
            "summary": {"total_classified": 1, "categories_found": ["revenue"]},
        }
        result = compute_missing_strategy(profile, classifications)
        assert result["strategies"][0]["treatment_type"] == "transform"  # not drop


# ── Readiness Score ──────────────────────────────────────────────────────────

class TestReadinessScore:
    def _base_inputs(self):
        profile = {
            "data": {
                "columns": {
                    "id": {"null_percent": 0, "unique_count": 100, "unique_percent": 100, "type_category": "numeric"},
                    "name": {"null_percent": 5, "unique_count": 80, "unique_percent": 80, "type_category": "string"},
                }
            }
        }
        so = {"duplicate_pct": 0, "constant_columns": [], "columns_fully_null": 0, "suspicious_id_columns": []}
        quality = {"overall_score": 85, "tables": {}}
        gdpr = {"total_pii_findings": 0, "overall_risk": "low", "has_special_category_data": False, "gdpr_categories": {}}
        kpis = [{"name": "KPI1", "status": "ready"}]
        cc = {"by_category": {"revenue": [{"table": "data", "column": "id", "confidence": 0.8}],
                               "timestamp": [{"table": "data", "column": "name", "confidence": 0.7}]},
              "summary": {"total_classified": 2, "categories_found": ["revenue", "timestamp"]}}
        return profile, so, quality, {}, gdpr, kpis, cc

    def test_green_score(self):
        from app.analysis.readiness_score import compute_readiness_score
        profile, so, quality, pii, gdpr, kpis, cc = self._base_inputs()
        result = compute_readiness_score(profile, so, quality, pii, gdpr, kpis, cc)
        assert result["total_score"] >= 70
        assert result["grade"] == "green"

    def test_red_with_special_category(self):
        from app.analysis.readiness_score import compute_readiness_score
        profile, so, quality, pii, gdpr, kpis, cc = self._base_inputs()
        gdpr["has_special_category_data"] = True
        gdpr["overall_risk"] = "critical"
        gdpr["total_pii_findings"] = 15
        gdpr["gdpr_categories"] = {f"cat{i}": {} for i in range(5)}
        so["duplicate_pct"] = 30
        so["constant_columns"] = ["a", "b", "c", "d"]
        so["columns_fully_null"] = 3
        result = compute_readiness_score(profile, so, quality, pii, gdpr, kpis, cc)
        assert result["total_score"] < 70

    def test_five_components(self):
        from app.analysis.readiness_score import compute_readiness_score
        profile, so, quality, pii, gdpr, kpis, cc = self._base_inputs()
        result = compute_readiness_score(profile, so, quality, pii, gdpr, kpis, cc)
        comps = result["components"]
        assert len(comps) == 5
        assert all(c["max"] == 20 for c in comps.values())
        assert sum(c["score"] for c in comps.values()) == result["total_score"]
