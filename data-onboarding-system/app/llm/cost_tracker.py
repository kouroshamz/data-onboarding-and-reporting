"""Token / cost tracking for LLM calls."""

from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import List

from loguru import logger


# Approximate pricing per 1M tokens (USD) — updated Feb 2026
MODEL_PRICING = {
    # OpenAI
    "gpt-4o-mini":        {"input": 0.15,  "output": 0.60},
    "gpt-4o":             {"input": 2.50,  "output": 10.00},
    "gpt-4.5-preview":    {"input": 75.00, "output": 150.00},
    "o3-mini":            {"input": 1.10,  "output": 4.40},
    # Anthropic
    "claude-3-5-haiku-20241022": {"input": 0.80, "output": 4.00},
    "claude-3-5-sonnet-20241022": {"input": 3.00, "output": 15.00},
    "claude-3-7-sonnet-20250219": {"input": 3.00, "output": 15.00},
    # Local — free
    "llama3.2":           {"input": 0.0,   "output": 0.0},
}


@dataclass
class UsageEntry:
    layer: str
    model: str
    input_tokens: int
    output_tokens: int
    latency_ms: float
    cost_usd: float
    cached: bool = False
    timestamp: str = ""


class CostTracker:
    """Accumulate LLM token usage and cost across a pipeline run."""

    def __init__(self, budget_limit_usd: float = 1.0):
        self.budget_limit_usd = budget_limit_usd
        self.entries: List[UsageEntry] = []

    @property
    def total_cost(self) -> float:
        return sum(e.cost_usd for e in self.entries)

    @property
    def total_input_tokens(self) -> int:
        return sum(e.input_tokens for e in self.entries)

    @property
    def total_output_tokens(self) -> int:
        return sum(e.output_tokens for e in self.entries)

    @property
    def budget_remaining(self) -> float:
        return max(0.0, self.budget_limit_usd - self.total_cost)

    @property
    def budget_exhausted(self) -> bool:
        return self.total_cost >= self.budget_limit_usd

    @staticmethod
    def estimate_cost(model: str, input_tokens: int, output_tokens: int) -> float:
        """Estimate cost in USD."""
        pricing = MODEL_PRICING.get(model, {"input": 0.15, "output": 0.60})
        cost = (input_tokens * pricing["input"] + output_tokens * pricing["output"]) / 1_000_000
        return round(cost, 6)

    def record(
        self,
        layer: str,
        model: str,
        input_tokens: int,
        output_tokens: int,
        latency_ms: float,
        cached: bool = False,
    ) -> UsageEntry:
        """Record a call and return the entry."""
        cost = 0.0 if cached else self.estimate_cost(model, input_tokens, output_tokens)
        entry = UsageEntry(
            layer=layer,
            model=model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            latency_ms=latency_ms,
            cost_usd=cost,
            cached=cached,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )
        self.entries.append(entry)
        return entry

    def to_dict(self) -> dict:
        """Serialize for JSON output."""
        return {
            "total_calls": len(self.entries),
            "total_input_tokens": self.total_input_tokens,
            "total_output_tokens": self.total_output_tokens,
            "total_cost_usd": round(self.total_cost, 6),
            "budget_limit_usd": self.budget_limit_usd,
            "budget_remaining_usd": round(self.budget_remaining, 6),
            "entries": [asdict(e) for e in self.entries],
        }

    def save(self, path: Path) -> None:
        """Write usage report to file."""
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, indent=2)
        logger.info(
            "LLM usage: {} calls, ${:.4f} spent (${:.4f} budget remaining)",
            len(self.entries), self.total_cost, self.budget_remaining,
        )
