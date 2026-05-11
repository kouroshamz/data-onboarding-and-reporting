"""Tests for LLM foundation — client factory, cache, cost tracker."""

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from app.llm.client import (
    BaseLLMClient,
    LLMResponse,
    create_llm_client,
)
from app.llm.cache import ResponseCache
from app.llm.cost_tracker import CostTracker


# -----------------------------------------------------------------------
# Client factory
# -----------------------------------------------------------------------

class TestCreateClient:
    def test_unknown_provider_raises(self):
        with pytest.raises(ValueError, match="Unknown LLM provider"):
            create_llm_client(provider="foobar", model="x")

    @patch("app.llm.client.OpenAIClient.__init__", return_value=None)
    def test_openai_provider(self, mock_init):
        client = create_llm_client("openai", "gpt-4o-mini", api_key="sk-test")
        assert client is not None

    @patch("app.llm.client.AnthropicClient.__init__", return_value=None)
    def test_anthropic_provider(self, mock_init):
        client = create_llm_client("anthropic", "claude-3-5-haiku-20241022", api_key="sk-test")
        assert client is not None

    @patch("app.llm.client.LocalClient.__init__", return_value=None)
    def test_local_provider(self, mock_init):
        client = create_llm_client("local", "llama3.2")
        assert client is not None


# -----------------------------------------------------------------------
# Response cache
# -----------------------------------------------------------------------

class TestResponseCache:
    def test_cache_miss_returns_none(self, tmp_path):
        cache = ResponseCache(tmp_path / "cache")
        assert cache.get("nonexistent") is None

    def test_cache_roundtrip(self, tmp_path):
        cache = ResponseCache(tmp_path / "cache")
        cache.put("key1", '{"result": "ok"}', meta={"layer": "test"})
        assert cache.get("key1") == '{"result": "ok"}'

    def test_cache_clear(self, tmp_path):
        cache = ResponseCache(tmp_path / "cache")
        cache.put("a", "1")
        cache.put("b", "2")
        cleared = cache.clear()
        assert cleared == 2
        assert cache.get("a") is None


# -----------------------------------------------------------------------
# Cost tracker
# -----------------------------------------------------------------------

class TestCostTracker:
    def test_record_and_totals(self):
        tracker = CostTracker(budget_limit_usd=1.0)
        tracker.record("test_layer", "gpt-4o-mini", 1000, 500, 200.0)
        assert tracker.total_input_tokens == 1000
        assert tracker.total_output_tokens == 500
        assert tracker.total_cost > 0
        assert not tracker.budget_exhausted

    def test_budget_exhaustion(self):
        tracker = CostTracker(budget_limit_usd=0.0001)
        # Record enough to blow budget
        tracker.record("test", "gpt-4o", 100000, 100000, 1000.0)
        assert tracker.budget_exhausted

    def test_cached_call_is_free(self):
        tracker = CostTracker()
        tracker.record("test", "gpt-4o-mini", 1000, 500, 0, cached=True)
        assert tracker.total_cost == 0.0

    def test_to_dict(self):
        tracker = CostTracker()
        tracker.record("l1", "gpt-4o-mini", 100, 50, 10.0)
        d = tracker.to_dict()
        assert "total_calls" in d
        assert d["total_calls"] == 1
        assert "entries" in d

    def test_save_to_file(self, tmp_path):
        tracker = CostTracker()
        tracker.record("l1", "gpt-4o-mini", 100, 50, 10.0)
        path = tmp_path / "usage.json"
        tracker.save(path)
        assert path.exists()
        data = json.loads(path.read_text())
        assert data["total_calls"] == 1

    def test_estimate_cost_known_model(self):
        cost = CostTracker.estimate_cost("gpt-4o-mini", 1_000_000, 1_000_000)
        # $0.15 input + $0.60 output = $0.75
        assert abs(cost - 0.75) < 0.01

    def test_estimate_cost_unknown_model_uses_default(self):
        cost = CostTracker.estimate_cost("some-future-model", 1_000_000, 1_000_000)
        assert cost > 0  # Uses default pricing
