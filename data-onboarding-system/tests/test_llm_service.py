"""Tests for LLMService — mocked LLM provider, full layer integration."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from app.llm.client import BaseLLMClient, LLMResponse
from app.llm.service import LLMService
from app.llm.schemas import TypeInspectorResult, InsightDetectorResult, ReportAdvisorResult


# -----------------------------------------------------------------------
# Mock LLM client that returns configurable JSON
# -----------------------------------------------------------------------

class MockLLMClient(BaseLLMClient):
    """Returns pre-configured JSON responses."""

    def __init__(self, responses: dict[str, str] | None = None):
        self._responses = responses or {}
        self._call_count = 0

    def name(self) -> str:
        return "mock/test-model"

    def chat(self, messages, *, temperature=0.1, max_tokens=2000, response_json=True):
        self._call_count += 1
        # Determine which layer based on system prompt content
        system = messages[0]["content"] if messages else ""
        for key, resp in self._responses.items():
            if key.lower() in system.lower():
                return LLMResponse(
                    content=resp, model="mock-model",
                    input_tokens=100, output_tokens=50, latency_ms=10.0,
                )
        # Default empty response
        return LLMResponse(
            content='{"findings": [], "insights": [], "good_to_know": [], "executive_summary": ""}',
            model="mock-model", input_tokens=50, output_tokens=20, latency_ms=5.0,
        )


# -----------------------------------------------------------------------
# Service disabled
# -----------------------------------------------------------------------

class TestServiceDisabled:
    def test_disabled_by_default(self):
        service = LLMService(None)
        assert not service.enabled

    def test_disabled_config(self):
        service = LLMService({"enabled": False})
        assert not service.enabled

    def test_all_layers_skip_when_disabled(self):
        service = LLMService(None)
        df = pd.DataFrame({"a": [1, 2, 3]})
        assert service.inspect_types("t", {}, df).skipped
        assert service.detect_insights({}, {}, {}, {}, {}, [], {}).skipped
        assert service.advise_report({}, {}, {}, {}, [], {}).skipped

    def test_usage_dict_when_disabled(self):
        service = LLMService(None)
        usage = service.usage_dict()
        assert usage["total_calls"] == 0


# -----------------------------------------------------------------------
# Service with mock client
# -----------------------------------------------------------------------

def _make_service_with_mock(mock_client, tmp_path):
    """Create an LLMService and inject a mock client."""
    service = LLMService({"enabled": False})  # Start disabled
    service._enabled = True
    service._client = mock_client
    service._cache = None  # No caching for tests
    service._layers = {"type_inspector": True, "insight_detector": True, "report_advisor": True}
    return service


class TestTypeInspector:
    def test_no_suspicious_columns_skips(self, tmp_path):
        mock = MockLLMClient()
        service = _make_service_with_mock(mock, tmp_path)

        profile = {"columns": {"age": {"type_category": "numeric", "dtype": "int64"}}}
        df = pd.DataFrame({"age": [25, 30, 35]})
        result = service.inspect_types("test_table", profile, df)
        assert result.skipped
        assert "no_suspicious" in result.reason

    def test_json_column_detected(self, tmp_path):
        mock_response = json.dumps({
            "findings": [{
                "column": "metadata",
                "current_type": "object",
                "detected_type": "json_object",
                "confidence": 0.95,
                "severity": "warning",
                "recommendation": "Parse JSON",
                "action": "parse_json",
            }]
        })
        mock = MockLLMClient({"data engineer": mock_response})
        service = _make_service_with_mock(mock, tmp_path)

        profile = {
            "columns": {
                "metadata": {
                    "type_category": "string", "dtype": "object",
                    "null_percent": 1, "unique_count": 50,
                    "unique_percent": 80,
                    "patterns": {"min_length": 10, "max_length": 500, "avg_length": 100},
                    "top_values": [],
                },
            }
        }
        df = pd.DataFrame({
            "metadata": ['{"key":"val"}', '{"key":"val2"}', '{"a":1}'],
        })
        result = service.inspect_types("orders", profile, df)
        assert not result.skipped
        assert len(result.findings) == 1
        assert result.findings[0].detected_type == "json_object"

    def test_invalid_column_names_filtered(self, tmp_path):
        """LLM returns a column name that doesn't exist — should be filtered."""
        mock_response = json.dumps({
            "findings": [{
                "column": "nonexistent_col",
                "current_type": "object",
                "detected_type": "json_object",
                "confidence": 0.9,
                "severity": "warning",
                "recommendation": "test",
                "action": "none",
            }]
        })
        mock = MockLLMClient({"data engineer": mock_response})
        service = _make_service_with_mock(mock, tmp_path)

        profile = {
            "columns": {
                "status": {
                    "type_category": "string", "dtype": "object",
                    "null_percent": 0, "unique_count": 3,
                    "unique_percent": 0.5,
                    "patterns": {"min_length": 4, "max_length": 10, "avg_length": 6},
                    "top_values": [],
                },
            }
        }
        df = pd.DataFrame({"status": ["active", "pending", "shipped"]})
        result = service.inspect_types("t1", profile, df)
        # The finding references "nonexistent_col" but our table only has "status"
        assert len(result.findings) == 0


class TestInsightDetector:
    def test_returns_insights(self, tmp_path):
        mock_response = json.dumps({
            "insights": [{
                "category": "data_scope",
                "severity": "warning",
                "title": "Narrow date range",
                "detail": "Only 4 days of data",
                "affected_tables": ["orders"],
                "recommendation": "Get more data",
            }],
            "good_to_know": ["98% FK match rate"],
            "executive_summary": "The dataset is small.",
        })
        mock = MockLLMClient({"data analyst": mock_response})
        service = _make_service_with_mock(mock, tmp_path)

        result = service.detect_insights(
            schema_data={"table_count": 1, "tables": {"orders": {"row_count": 100, "columns": []}}},
            profile_results={"orders": {"sample_size": 100, "completeness_score": 90, "columns": {}}},
            quality_data={"overall_score": 75, "tables": {"orders": {"overall_score": 75, "severity_counts": {}}}},
            pii_data={"summary": {"has_pii": False}, "by_table": {}},
            relationships={"relationships": []},
            kpis=[],
            industry={"industry": "general"},
        )
        assert not result.skipped
        assert len(result.insights) == 1
        assert result.insights[0].title == "Narrow date range"
        assert len(result.good_to_know) == 1

    def test_layer_disabled(self, tmp_path):
        mock = MockLLMClient()
        service = _make_service_with_mock(mock, tmp_path)
        service._layers["insight_detector"] = False
        result = service.detect_insights({}, {}, {}, {}, {}, [], {})
        assert result.skipped


class TestReportAdvisor:
    def test_returns_layout(self, tmp_path):
        mock_response = json.dumps({
            "hero_metric": {
                "label": "Quality", "value": "85/100",
                "color": "green", "commentary": "Good",
            },
            "section_order": ["executive_summary", "quality_dashboard", "kpi_recommendations"],
            "sections": {
                "executive_summary": {
                    "emphasis": "high",
                    "narrative": "This dataset is in good shape.",
                },
                "quality_dashboard": {
                    "emphasis": "medium",
                    "narrative": "Quality is above average.",
                },
            },
            "generation_notes": "Quality is strong, so KPIs come second.",
        })
        mock = MockLLMClient({"report layout advisor": mock_response})
        service = _make_service_with_mock(mock, tmp_path)

        result = service.advise_report(
            schema_data={"table_count": 1, "tables": {}},
            quality_data={"overall_score": 85, "tables": {}},
            pii_data={"summary": {"has_pii": False}},
            relationships={"relationships": []},
            kpis=[{"name": "Revenue", "status": "ready"}],
            industry={"industry": "e_commerce", "confidence": 0.9},
        )
        assert not result.skipped
        assert result.layout.hero_metric.label == "Quality"
        assert "executive_summary" in result.layout.section_order
        assert result.generation_notes != ""

    def test_layer_disabled(self, tmp_path):
        mock = MockLLMClient()
        service = _make_service_with_mock(mock, tmp_path)
        service._layers["report_advisor"] = False
        result = service.advise_report({}, {}, {}, {}, [], {})
        assert result.skipped


# -----------------------------------------------------------------------
# Budget enforcement
# -----------------------------------------------------------------------

class TestBudgetEnforcement:
    def test_skips_when_budget_exhausted(self, tmp_path):
        mock = MockLLMClient()
        service = _make_service_with_mock(mock, tmp_path)
        service._cost._budget_limit_usd = 0.0  # zero budget
        # Force budget_exhausted
        service._cost.record("prior", "gpt-4o", 100000, 100000, 100.0)

        result = service.detect_insights({}, {}, {}, {}, {}, [], {})
        assert result.skipped
        assert "llm_call_failed" in result.reason or "budget" in result.reason


# -----------------------------------------------------------------------
# Usage tracking
# -----------------------------------------------------------------------

class TestUsageTracking:
    def test_usage_accumulates(self, tmp_path):
        mock_response = json.dumps({
            "insights": [],
            "good_to_know": [],
            "executive_summary": "",
        })
        mock = MockLLMClient({"data analyst": mock_response})
        service = _make_service_with_mock(mock, tmp_path)

        service.detect_insights(
            {"table_count": 0, "tables": {}}, {}, {"overall_score": 0, "tables": {}},
            {"summary": {}, "by_table": {}}, {"relationships": []}, [],
            {"industry": "general"},
        )

        usage = service.usage_dict()
        assert usage["total_calls"] >= 1

    def test_save_usage(self, tmp_path):
        mock = MockLLMClient()
        service = _make_service_with_mock(mock, tmp_path)
        path = tmp_path / "usage.json"
        service.save_usage(path)
        assert path.exists()
