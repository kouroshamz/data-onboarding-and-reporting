"""Mocked LLM integration tests — full service workflow without real API calls."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from app.llm.client import BaseLLMClient, LLMResponse
from app.llm.cache import ResponseCache
from app.llm.cost_tracker import CostTracker
from app.llm.service import LLMService, _try_float
from app.llm.schemas import (
    InsightDetectorResult,
    ReportAdvisorResult,
    TypeFinding,
    TypeInspectorResult,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _fake_llm_config(tmp_path, enabled=True, cache=True):
    return {
        "enabled": enabled,
        "provider": "openai",
        "model": "gpt-4o-mini",
        "api_key": "sk-test-key",
        "temperature": 0.1,
        "max_tokens": 2000,
        "budget_limit_usd": 1.0,
        "layers": {
            "type_inspector": True,
            "insight_detector": True,
            "report_advisor": True,
        },
        "cache": {"enabled": cache, "directory": str(tmp_path / ".llm_cache")},
    }


TYPE_INSPECTOR_RESPONSE = json.dumps({
    "findings": [
        {
            "column": "price",
            "current_type": "object",
            "detected_type": "numeric_as_string",
            "confidence": 0.95,
            "severity": "critical",
            "recommendation": "Convert to DECIMAL",
            "action": "convert_numeric",
            "details": {},
        }
    ]
})

INSIGHT_RESPONSE = json.dumps({
    "insights": [
        {
            "category": "data_scope",
            "severity": "info",
            "title": "Dataset covers 3 years",
            "detail": "The date range spans 2020-2023.",
            "affected_tables": ["orders"],
            "recommendation": "Verify expected range with client.",
        }
    ],
    "good_to_know": ["All prices are in USD"],
    "executive_summary": "The dataset is well-structured.",
})

ADVISOR_RESPONSE = json.dumps({
    "hero_metric": {"label": "Quality Score", "value": "85/100", "color": "green"},
    "section_order": ["executive_summary", "quality"],
    "sections": {
        "executive_summary": {"emphasis": "high", "narrative": "Good data."},
    },
    "generation_notes": "Layout optimized for ecommerce.",
})


@pytest.fixture
def mock_client():
    """A BaseLLMClient mock that returns JSON strings."""
    client = MagicMock(spec=BaseLLMClient)
    client.name.return_value = "openai/gpt-4o-mini"
    return client


def _make_response(content, tokens_in=100, tokens_out=50):
    return LLMResponse(
        content=content,
        model="gpt-4o-mini",
        input_tokens=tokens_in,
        output_tokens=tokens_out,
        latency_ms=200.0,
    )


# ---------------------------------------------------------------------------
# Service initialisation
# ---------------------------------------------------------------------------

class TestLLMServiceInit:

    def test_disabled_when_no_config(self):
        svc = LLMService(None)
        assert not svc.enabled

    def test_disabled_when_enabled_false(self, tmp_path):
        svc = LLMService(_fake_llm_config(tmp_path, enabled=False))
        assert not svc.enabled

    @patch("app.llm.service.create_llm_client")
    def test_enabled_with_valid_config(self, mock_factory, tmp_path):
        mock_factory.return_value = MagicMock(spec=BaseLLMClient)
        mock_factory.return_value.name.return_value = "openai/gpt-4o-mini"
        svc = LLMService(_fake_llm_config(tmp_path))
        assert svc.enabled

    @patch("app.llm.service.create_llm_client")
    def test_cache_initialised_when_configured(self, mock_factory, tmp_path):
        mock_factory.return_value = MagicMock(spec=BaseLLMClient)
        mock_factory.return_value.name.return_value = "openai/gpt-4o-mini"
        svc = LLMService(_fake_llm_config(tmp_path, cache=True))
        assert svc._cache is not None

    @patch("app.llm.service.create_llm_client")
    def test_cache_disabled(self, mock_factory, tmp_path):
        mock_factory.return_value = MagicMock(spec=BaseLLMClient)
        mock_factory.return_value.name.return_value = "openai/gpt-4o-mini"
        svc = LLMService(_fake_llm_config(tmp_path, cache=False))
        assert svc._cache is None


# ---------------------------------------------------------------------------
# Type Inspector (L1) — mocked
# ---------------------------------------------------------------------------

class TestTypeInspectorMocked:

    @pytest.fixture
    def svc(self, tmp_path, mock_client):
        mock_client.chat.return_value = _make_response(TYPE_INSPECTOR_RESPONSE)
        with patch("app.llm.service.create_llm_client", return_value=mock_client):
            return LLMService(_fake_llm_config(tmp_path))

    def test_inspect_types_returns_findings(self, svc):
        profile = {
            "columns": {
                "price": {
                    "dtype": "object", "type_category": "string",
                    "null_percent": 0, "unique_count": 100, "unique_percent": 50,
                    "patterns": {"min_length": 3, "max_length": 8, "avg_length": 5},
                    "top_values": [{"value": "12.99", "count": 5}],
                },
            }
        }
        sample = pd.DataFrame({"price": ["12.99", "3.50", "149.00", "0.99", "25.00"] * 4})
        result = svc.inspect_types("orders", profile, sample)
        assert not result.skipped
        assert len(result.findings) >= 1
        assert result.findings[0].detected_type == "numeric_as_string"

    def test_inspect_types_skips_when_nothing_suspicious(self, svc):
        profile = {"columns": {"name": {"dtype": "object", "type_category": "string",
                                         "null_percent": 0, "unique_count": 500, "unique_percent": 100,
                                         "patterns": {"min_length": 3, "max_length": 10, "avg_length": 6}}}}
        sample = pd.DataFrame({"name": ["Alice", "Bob", "Charlie"]})
        result = svc.inspect_types("users", profile, sample)
        assert result.skipped


# ---------------------------------------------------------------------------
# Insight Detector (L2) — mocked
# ---------------------------------------------------------------------------

class TestInsightDetectorMocked:

    @pytest.fixture
    def svc(self, tmp_path, mock_client):
        mock_client.chat.return_value = _make_response(INSIGHT_RESPONSE)
        with patch("app.llm.service.create_llm_client", return_value=mock_client):
            return LLMService(_fake_llm_config(tmp_path))

    def test_detect_insights_returns_results(self, svc):
        schema = {"table_count": 1, "tables": {"orders": {"row_count": 100, "columns": []}}}
        profiles = {"orders": {"columns": {}, "completeness_score": 90, "sample_size": 100}}
        quality = {"overall_score": 85, "tables": {"orders": {"overall_score": 85, "severity_counts": {}}}}
        pii = {"summary": {"has_pii": False}, "by_table": {"orders": {"has_pii": False, "pii_columns": []}}}
        result = svc.detect_insights(schema, profiles, quality, pii, {"relationships": []}, [], {"industry": "general"})
        assert not result.skipped
        assert len(result.insights) >= 1
        assert result.executive_summary != ""

    def test_insights_good_to_know(self, svc):
        schema = {"table_count": 1, "tables": {"t": {"row_count": 100, "columns": []}}}
        profiles = {"t": {"columns": {}, "completeness_score": 90, "sample_size": 100}}
        quality = {"overall_score": 80, "tables": {"t": {"overall_score": 80, "severity_counts": {"warning": 1}}}}
        pii = {"summary": {"has_pii": False}, "by_table": {"t": {"has_pii": False, "pii_columns": []}}}
        result = svc.detect_insights(schema, profiles, quality, pii, {"relationships": []}, [], {"industry": "general"})
        assert len(result.good_to_know) >= 1


# ---------------------------------------------------------------------------
# Report Advisor (L3) — mocked
# ---------------------------------------------------------------------------

class TestReportAdvisorMocked:

    @pytest.fixture
    def svc(self, tmp_path, mock_client):
        mock_client.chat.return_value = _make_response(ADVISOR_RESPONSE)
        with patch("app.llm.service.create_llm_client", return_value=mock_client):
            return LLMService(_fake_llm_config(tmp_path))

    def test_advise_report_returns_layout(self, svc):
        result = svc.advise_report(
            schema_data={"table_count": 1, "tables": {}},
            quality_data={"overall_score": 85},
            pii_data={"summary": {"has_pii": False}, "by_table": {}},
            relationships={"relationships": []},
            kpis=[], industry={"industry": "ecommerce"},
        )
        assert not result.skipped
        assert result.layout is not None


# ---------------------------------------------------------------------------
# Caching
# ---------------------------------------------------------------------------

class TestLLMCaching:

    def test_cache_stores_and_retrieves(self, tmp_path):
        cache = ResponseCache(tmp_path / ".cache")
        key = json.dumps([{"role": "user", "content": "hello"}])
        cache.put(key, '{"result": "world"}', meta={"layer": "test"})
        assert cache.get(key) == '{"result": "world"}'

    def test_cache_miss_returns_none(self, tmp_path):
        cache = ResponseCache(tmp_path / ".cache")
        assert cache.get("nonexistent") is None

    def test_cache_clear(self, tmp_path):
        cache = ResponseCache(tmp_path / ".cache")
        cache.put("k1", "v1")
        cache.put("k2", "v2")
        cleared = cache.clear()
        assert cleared == 2
        assert cache.get("k1") is None

    @patch("app.llm.service.create_llm_client")
    def test_service_uses_cache(self, mock_factory, tmp_path, mock_client):
        """Second call should hit cache, not call LLM again."""
        mock_client.chat.return_value = _make_response(TYPE_INSPECTOR_RESPONSE)
        mock_factory.return_value = mock_client
        svc = LLMService(_fake_llm_config(tmp_path, cache=True))

        profile = {
            "columns": {
                "price": {
                    "dtype": "object", "type_category": "string",
                    "null_percent": 0, "unique_count": 100, "unique_percent": 50,
                    "patterns": {"min_length": 3, "max_length": 8, "avg_length": 5},
                    "top_values": [{"value": "12.99", "count": 5}],
                },
            }
        }
        sample = pd.DataFrame({"price": ["12.99", "3.50", "149.00", "0.99", "25.00"] * 4})

        r1 = svc.inspect_types("orders", profile, sample)
        r2 = svc.inspect_types("orders", profile, sample)
        # The LLM chat should have been called only once
        assert mock_client.chat.call_count == 1
        assert not r1.skipped
        assert not r2.skipped


# ---------------------------------------------------------------------------
# Cost tracking
# ---------------------------------------------------------------------------

class TestCostTracker:

    def test_record_and_totals(self):
        ct = CostTracker(budget_limit_usd=1.0)
        ct.record(layer="test", model="gpt-4o-mini", input_tokens=100,
                  output_tokens=50, latency_ms=200)
        d = ct.to_dict()
        assert d["total_calls"] == 1
        assert d["total_input_tokens"] + d["total_output_tokens"] > 0

    def test_budget_exhausted(self):
        ct = CostTracker(budget_limit_usd=0.0001)
        ct.record(layer="test", model="gpt-4o-mini", input_tokens=100000,
                  output_tokens=50000, latency_ms=200)
        assert ct.budget_exhausted

    def test_save_and_load(self, tmp_path):
        ct = CostTracker()
        ct.record(layer="test", model="m", input_tokens=10, output_tokens=5, latency_ms=100)
        path = tmp_path / "usage.json"
        ct.save(path)
        data = json.loads(path.read_text())
        assert data["total_calls"] == 1


# ---------------------------------------------------------------------------
# Cross-validation
# ---------------------------------------------------------------------------

class TestCrossValidation:

    def test_numeric_as_string_boost(self):
        finding = TypeFinding(
            column="amount", current_type="object", detected_type="numeric_as_string",
            confidence=0.8, severity="critical", recommendation="Convert",
        )
        result = TypeInspectorResult(findings=[finding])
        profile = {"columns": {"amount": {}}}
        sample = pd.DataFrame({"amount": ["12.99", "3.50", "0.99", "25.00", "149.00"]})
        validated = LLMService.cross_validate_type_findings(result, profile, sample)
        assert validated.findings[0].confidence > 0.8

    def test_numeric_as_string_penalise(self):
        finding = TypeFinding(
            column="notes", current_type="object", detected_type="numeric_as_string",
            confidence=0.65, severity="warning", recommendation="Check",
        )
        result = TypeInspectorResult(findings=[finding])
        profile = {"columns": {"notes": {}}}
        sample = pd.DataFrame({"notes": ["hello", "world", "foo", "bar", "baz"]})
        validated = LLMService.cross_validate_type_findings(result, profile, sample)
        assert validated.findings[0].confidence < 0.65

    def test_boolean_as_string_boost(self):
        finding = TypeFinding(
            column="active", current_type="object", detected_type="boolean_as_string",
            confidence=0.75, severity="warning", recommendation="Cast to bool",
        )
        result = TypeInspectorResult(findings=[finding])
        profile = {"columns": {"active": {"unique_count": 2}}}
        sample = pd.DataFrame({"active": ["true", "false", "true"]})
        validated = LLMService.cross_validate_type_findings(result, profile, sample)
        assert validated.findings[0].confidence > 0.75

    def test_try_float_helper(self):
        assert _try_float("12.99")
        assert _try_float("1,000")
        assert not _try_float("hello")
        assert not _try_float("")
