"""Tests for LLM prompt builders and heuristic pre-filter."""

import pandas as pd
import pytest

from app.llm.prompts.type_inspector import (
    needs_llm_inspection,
    collect_suspicious_columns,
    build_type_inspector_messages,
)
from app.llm.prompts.insight_detector import (
    build_dataset_summary,
    build_insight_detector_messages,
)
from app.llm.prompts.report_advisor import (
    build_report_context,
    build_report_advisor_messages,
)


# -----------------------------------------------------------------------
# Heuristic pre-filter
# -----------------------------------------------------------------------

class TestHeuristicFilter:
    def test_json_values_flagged(self):
        profile = {"unique_percent": 90, "unique_count": 100}
        vals = ['{"key": "val"}', '{"key": "val2"}', '{"key": "val3"}']
        assert needs_llm_inspection(profile, vals) is True

    def test_numeric_as_string_flagged(self):
        profile = {"unique_percent": 80, "unique_count": 50}
        vals = ["42.5", "100", "3.14", "99.9", "0.1"]
        assert needs_llm_inspection(profile, vals) is True

    def test_low_cardinality_flagged(self):
        profile = {"unique_percent": 0.5, "unique_count": 4}
        vals = ["active", "pending", "cancelled", "shipped"]
        assert needs_llm_inspection(profile, vals) is True

    def test_date_strings_flagged(self):
        profile = {"unique_percent": 90, "unique_count": 50}
        vals = ["2024-01-15", "2024-02-20", "2024-03-10"]
        assert needs_llm_inspection(profile, vals) is True

    def test_boolean_strings_flagged(self):
        profile = {"unique_percent": 50, "unique_count": 2}
        vals = ["true", "false", "True", "False"]
        assert needs_llm_inspection(profile, vals) is True

    def test_normal_strings_not_flagged(self):
        profile = {"unique_percent": 85, "unique_count": 200, "patterns": {"avg_length": 15}}
        vals = ["John Smith", "Jane Doe", "Alice Johnson", "Bob Williams"]
        assert needs_llm_inspection(profile, vals) is False

    def test_empty_values_not_flagged(self):
        profile = {"unique_percent": 0, "unique_count": 0}
        vals = [None, None, None]
        assert needs_llm_inspection(profile, vals) is False

    def test_long_strings_flagged(self):
        profile = {"unique_percent": 90, "unique_count": 50, "patterns": {"avg_length": 600}}
        vals = ["<html><body>long content</body></html>"]
        assert needs_llm_inspection(profile, vals) is True


# -----------------------------------------------------------------------
# Type inspector prompt builder
# -----------------------------------------------------------------------

class TestTypeInspectorPrompt:
    def test_builds_messages_with_system_and_user(self):
        columns = [{"name": "status", "dtype": "object", "sample_values": ["active"]}]
        msgs = build_type_inspector_messages("orders", columns)
        assert len(msgs) == 2
        assert msgs[0]["role"] == "system"
        assert msgs[1]["role"] == "user"
        assert "orders" in msgs[1]["content"]

    def test_collect_suspicious_finds_json_column(self):
        profile = {
            "columns": {
                "metadata": {
                    "type_category": "string",
                    "dtype": "object",
                    "null_percent": 1.0,
                    "unique_count": 50,
                    "unique_percent": 80,
                    "patterns": {"min_length": 10, "max_length": 500, "avg_length": 100},
                    "top_values": [],
                },
                "age": {
                    "type_category": "numeric",
                    "dtype": "int64",
                    "null_percent": 0,
                    "unique_count": 50,
                    "unique_percent": 80,
                },
            }
        }
        df = pd.DataFrame({
            "metadata": ['{"key":"val"}', '{"key":"val2"}', '{"a":1}'],
            "age": [25, 30, 35],
        })
        suspicious = collect_suspicious_columns("test_table", profile, df)
        # Should only flag metadata (string with JSON), not age (numeric)
        assert len(suspicious) >= 1
        names = [s["name"] for s in suspicious]
        assert "metadata" in names
        assert "age" not in names


# -----------------------------------------------------------------------
# Insight detector prompt
# -----------------------------------------------------------------------

class TestInsightDetectorPrompt:
    def _sample_data(self):
        return {
            "schema_data": {"table_count": 2, "tables": {
                "orders": {"row_count": 1000, "columns": [{"column_name": "id"}, {"column_name": "total"}]},
                "customers": {"row_count": 500, "columns": [{"column_name": "id"}]},
            }},
            "profile_results": {
                "orders": {
                    "sample_size": 1000, "completeness_score": 85.0,
                    "columns": {
                        "total": {
                            "type_category": "numeric", "null_percent": 2.0,
                            "unique_percent": 80, "unique_count": 800,
                            "statistics": {"min": 0, "max": 9999, "mean": 142, "std": 50},
                            "top_values": [],
                        },
                    },
                },
                "customers": {
                    "sample_size": 500, "completeness_score": 95.0,
                    "columns": {
                        "id": {
                            "type_category": "numeric", "null_percent": 0,
                            "unique_percent": 100, "unique_count": 500,
                            "top_values": [],
                        },
                    },
                },
            },
            "quality_data": {"overall_score": 75, "tables": {
                "orders": {"overall_score": 70, "severity_counts": {"warning": 2}},
                "customers": {"overall_score": 80, "severity_counts": {}},
            }},
            "pii_data": {"summary": {"has_pii": False, "tables_with_pii": 0, "total_pii_columns": 0, "risk_score": "none"}, "by_table": {}},
            "relationships": {"relationships": []},
            "kpis": [{"name": "Revenue", "status": "ready"}],
            "industry": {"industry": "e_commerce", "confidence": 0.8},
        }

    def test_build_summary_structure(self):
        d = self._sample_data()
        summary = build_dataset_summary(
            d["schema_data"], d["profile_results"], d["quality_data"],
            d["pii_data"], d["relationships"], d["kpis"], d["industry"],
        )
        assert "dataset_overview" in summary
        assert "tables" in summary
        assert summary["dataset_overview"]["total_tables"] == 2

    def test_build_messages(self):
        d = self._sample_data()
        summary = build_dataset_summary(
            d["schema_data"], d["profile_results"], d["quality_data"],
            d["pii_data"], d["relationships"], d["kpis"], d["industry"],
        )
        msgs = build_insight_detector_messages(summary)
        assert len(msgs) == 2
        assert msgs[0]["role"] == "system"


# -----------------------------------------------------------------------
# Report advisor prompt
# -----------------------------------------------------------------------

class TestReportAdvisorPrompt:
    def test_build_context(self):
        ctx = build_report_context(
            schema_data={"table_count": 3, "tables": {}},
            quality_data={"overall_score": 72, "tables": {
                "t1": {"overall_score": 60},
                "t2": {"overall_score": 85},
            }},
            pii_data={"summary": {"has_pii": True, "tables_with_pii": 1, "total_pii_columns": 2, "risk_score": "medium"}},
            relationships={"relationships": [{"table1": "a", "column1": "b", "table2": "c", "column2": "d"}]},
            kpis=[{"name": "KPI1", "status": "ready"}, {"name": "KPI2", "status": "partial"}],
            industry={"industry": "finance", "confidence": 0.9},
        )
        assert ctx["industry"] == "finance"
        assert ctx["quality"]["overall_score"] == 72
        assert ctx["pii"]["has_pii"] is True
        assert ctx["kpis"]["ready"] == 1
        assert ctx["relationships_count"] == 1

    def test_build_messages(self):
        ctx = build_report_context(
            schema_data={"table_count": 1, "tables": {}},
            quality_data={"overall_score": 90, "tables": {}},
            pii_data={"summary": {"has_pii": False}},
            relationships={"relationships": []},
            kpis=[],
            industry={"industry": "general"},
        )
        msgs = build_report_advisor_messages(ctx)
        assert len(msgs) == 2
        assert "report layout advisor" in msgs[0]["content"].lower()
