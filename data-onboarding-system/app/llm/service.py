"""LLM Service — orchestrates all three layers with caching and cost tracking.

Usage:
    service = LLMService(llm_config)
    # Returns TypeInspectorResult (or skipped result if disabled/failure)
    type_result = service.inspect_types(table_name, profile, sample_data)
    # Returns InsightDetectorResult
    insight_result = service.detect_insights(schema, profiles, quality, pii, rels, kpis, industry)
    # Returns ReportAdvisorResult
    advisor_result = service.advise_report(schema, quality, pii, rels, kpis, industry, insights, types)
    # Save usage report
    service.save_usage(output_dir / "llm_usage.json")
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from loguru import logger

from app.llm.client import BaseLLMClient, LLMResponse, create_llm_client
from app.llm.cache import ResponseCache
from app.llm.cost_tracker import CostTracker
from app.llm.schemas import (
    InsightDetectorResult,
    ReportAdvisorResult,
    TypeInspectorResult,
)
from app.llm.prompts.type_inspector import (
    build_type_inspector_messages,
    collect_suspicious_columns,
)
from app.llm.prompts.insight_detector import (
    build_dataset_summary,
    build_insight_detector_messages,
)
from app.llm.prompts.report_advisor import (
    build_report_advisor_messages,
    build_report_context,
)


def _try_float(v: str) -> bool:
    """Return True if *v* can be parsed as a float."""
    try:
        float(v.replace(",", ""))
        return True
    except (ValueError, TypeError):
        return False


class LLMService:
    """Facade for all LLM layers — handles caching, cost, retries, graceful skip."""

    def __init__(self, llm_config: Any = None):
        """Initialise from an LLMConfig pydantic model or dict.

        If *llm_config* is None or ``enabled`` is False, all layers return
        ``skipped=True`` results so the pipeline continues unaffected.
        """
        self._enabled = False
        self._client: Optional[BaseLLMClient] = None
        self._cache: Optional[ResponseCache] = None
        self._cost = CostTracker()
        self._temperature = 0.1
        self._max_tokens = 2000
        self._layers: Dict[str, bool] = {
            "type_inspector": True,
            "insight_detector": True,
            "report_advisor": True,
        }

        if llm_config is None:
            return

        # Accept dict or pydantic model
        cfg = llm_config if isinstance(llm_config, dict) else (
            llm_config.model_dump() if hasattr(llm_config, "model_dump") else
            llm_config.dict() if hasattr(llm_config, "dict") else
            vars(llm_config)
        )

        if not cfg.get("enabled", False):
            return

        # Resolve API key
        api_key_env = cfg.get("api_key_env", "OPENAI_API_KEY")
        api_key = cfg.get("api_key", "") or os.getenv(api_key_env, "")
        provider = cfg.get("provider", "openai")
        model = cfg.get("model", "gpt-4o-mini")
        base_url = cfg.get("base_url")

        if not api_key and provider not in ("local", "ollama"):
            logger.warning("LLM enabled but no API key found (env: {}). LLM layers will be skipped.", api_key_env)
            return

        try:
            self._client = create_llm_client(
                provider=provider,
                model=model,
                api_key=api_key,
                base_url=base_url,
            )
            self._enabled = True
            logger.info("LLM service active: {}", self._client.name())
        except Exception as exc:
            logger.warning("Failed to create LLM client: {}. Layers will be skipped.", exc)
            return

        self._temperature = cfg.get("temperature", 0.1)
        self._max_tokens = cfg.get("max_tokens", 2000)
        self._cost = CostTracker(budget_limit_usd=cfg.get("budget_limit_usd", 1.0))

        # Per-layer toggles
        layers = cfg.get("layers", {})
        if isinstance(layers, dict):
            for k in self._layers:
                self._layers[k] = layers.get(k, True)

        # Cache
        cache_cfg = cfg.get("cache", {})
        if isinstance(cache_cfg, dict) and cache_cfg.get("enabled", True):
            cache_dir = cache_cfg.get("directory", ".llm_cache")
            self._cache = ResponseCache(cache_dir)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _call_llm(
        self,
        layer: str,
        messages: List[Dict[str, str]],
    ) -> Optional[str]:
        """Call the LLM with caching, budget check, and error handling.

        Returns parsed content string or None on failure.
        """
        if not self._enabled or not self._client:
            return None

        if self._cost.budget_exhausted:
            logger.warning("LLM budget exhausted — skipping {} call", layer)
            return None

        # Check cache
        cache_key = json.dumps(messages, sort_keys=True, default=str)
        if self._cache:
            cached = self._cache.get(cache_key)
            if cached is not None:
                self._cost.record(
                    layer=layer,
                    model=self._client.name(),
                    input_tokens=0,
                    output_tokens=0,
                    latency_ms=0,
                    cached=True,
                )
                return cached

        # Call LLM
        try:
            resp: LLMResponse = self._client.chat(
                messages,
                temperature=self._temperature,
                max_tokens=self._max_tokens,
                response_json=True,
            )
        except Exception as exc:
            logger.error("LLM call failed for {}: {}", layer, exc)
            return None

        # Record usage
        cost_entry = self._cost.record(
            layer=layer,
            model=resp.model or self._client.name(),
            input_tokens=resp.input_tokens,
            output_tokens=resp.output_tokens,
            latency_ms=resp.latency_ms,
        )
        logger.info(
            "LLM [{}] {} tokens in / {} out, {:.0f}ms, ${:.4f}",
            layer, resp.input_tokens, resp.output_tokens,
            resp.latency_ms, cost_entry.cost_usd,
        )

        # Cache response
        if self._cache and resp.content:
            self._cache.put(cache_key, resp.content, meta={
                "layer": layer,
                "model": resp.model,
                "tokens": resp.input_tokens + resp.output_tokens,
            })

        return resp.content

    def _parse_json(self, raw: str) -> Optional[Dict[str, Any]]:
        """Parse JSON from LLM response, tolerating markdown fences."""
        text = raw.strip()
        # Strip markdown code fences
        if text.startswith("```"):
            lines = text.split("\n")
            text = "\n".join(lines[1:])
            if text.rstrip().endswith("```"):
                text = text.rstrip()[:-3]
        try:
            return json.loads(text)
        except json.JSONDecodeError as exc:
            logger.warning("LLM returned invalid JSON: {}", exc)
            return None

    # ------------------------------------------------------------------
    # Layer 1 — Type Inspector
    # ------------------------------------------------------------------

    def inspect_types(
        self,
        table_name: str,
        profile: Dict[str, Any],
        sample_data: pd.DataFrame,
    ) -> TypeInspectorResult:
        """Run L1 Type Inspector on a single table.

        Returns immediately with ``skipped=True`` if LLM is disabled,
        the layer is toggled off, or no suspicious columns found.
        """
        if not self._enabled or not self._layers.get("type_inspector"):
            return TypeInspectorResult(skipped=True, reason="llm_disabled")

        # Collect suspicious string columns
        suspicious = collect_suspicious_columns(table_name, profile, sample_data)
        if not suspicious:
            return TypeInspectorResult(skipped=True, reason="no_suspicious_columns")

        messages = build_type_inspector_messages(table_name, suspicious)
        raw = self._call_llm("type_inspector", messages)
        if raw is None:
            return TypeInspectorResult(skipped=True, reason="llm_call_failed")

        data = self._parse_json(raw)
        if data is None:
            return TypeInspectorResult(skipped=True, reason="invalid_json")

        # Validate column names against actual columns
        valid_cols = set(profile.get("columns", {}).keys())
        findings = data.get("findings", [])
        validated = [f for f in findings if f.get("column") in valid_cols]

        try:
            result = TypeInspectorResult(findings=[
                # Let Pydantic validate each finding
                __import__("app.llm.schemas", fromlist=["TypeFinding"]).TypeFinding(**f)
                for f in validated
            ])
            # Cross-validate against profiler evidence
            return self.cross_validate_type_findings(result, profile, sample_data)
        except Exception as exc:
            logger.warning("Type inspector result validation failed: {}", exc)
            return TypeInspectorResult(findings=[], skipped=False, reason=f"validation_error: {exc}")

    # ------------------------------------------------------------------
    # Cross-validation — boost / penalise confidence using profiler stats
    # ------------------------------------------------------------------

    @staticmethod
    def cross_validate_type_findings(
        result: "TypeInspectorResult",
        profile: Dict[str, Any],
        sample_data: pd.DataFrame,
    ) -> "TypeInspectorResult":
        """Adjust confidence of type findings using hard evidence from the profiler.

        - If detected_type is 'numeric_as_string' and ≥90% values parse → boost
        - If detected_type is 'date_as_string' and ≥50% match date regex → boost
        - If detected_type is 'boolean_as_string' and unique_count ≤ 3 → boost
        - Otherwise reduce confidence slightly (LLM was speculative)
        """
        import re as _re

        if result.skipped or not result.findings:
            return result

        for finding in result.findings:
            col = finding.column
            if col not in sample_data.columns:
                continue

            vals = sample_data[col].dropna().astype(str).tolist()
            if not vals:
                continue

            if finding.detected_type == "numeric_as_string":
                ok = sum(1 for v in vals if _try_float(v))
                pct = ok / len(vals) if vals else 0
                if pct >= 0.9:
                    finding.confidence = min(1.0, finding.confidence + 0.1)
                else:
                    finding.confidence = max(0.3, finding.confidence - 0.15)

            elif finding.detected_type == "date_as_string":
                date_re = _re.compile(r"\d{4}[-/]\d{1,2}[-/]\d{1,2}")
                hits = sum(1 for v in vals if date_re.search(v))
                pct = hits / len(vals) if vals else 0
                if pct >= 0.5:
                    finding.confidence = min(1.0, finding.confidence + 0.1)
                else:
                    finding.confidence = max(0.3, finding.confidence - 0.15)

            elif finding.detected_type == "boolean_as_string":
                col_profile = profile.get("columns", {}).get(col, {})
                if col_profile.get("unique_count", 999) <= 3:
                    finding.confidence = min(1.0, finding.confidence + 0.1)

        # Drop findings whose confidence fell below threshold
        result.findings = [f for f in result.findings if f.confidence >= 0.5]
        return result

    # ------------------------------------------------------------------
    # Layer 2 — Insight Detector
    # ------------------------------------------------------------------

    def detect_insights(
        self,
        schema_data: Dict[str, Any],
        profile_results: Dict[str, Any],
        quality_data: Dict[str, Any],
        pii_data: Dict[str, Any],
        relationships: Dict[str, Any],
        kpis: List[Dict[str, Any]],
        industry: Dict[str, Any],
    ) -> InsightDetectorResult:
        """Run L2 Insight Detector on the full dataset."""
        if not self._enabled or not self._layers.get("insight_detector"):
            return InsightDetectorResult(skipped=True, reason="llm_disabled")

        summary = build_dataset_summary(
            schema_data, profile_results, quality_data,
            pii_data, relationships, kpis, industry,
        )
        messages = build_insight_detector_messages(summary)
        raw = self._call_llm("insight_detector", messages)
        if raw is None:
            return InsightDetectorResult(skipped=True, reason="llm_call_failed")

        data = self._parse_json(raw)
        if data is None:
            return InsightDetectorResult(skipped=True, reason="invalid_json")

        try:
            return InsightDetectorResult(**data)
        except Exception as exc:
            logger.warning("Insight detector result validation failed: {}", exc)
            return InsightDetectorResult(
                insights=[],
                good_to_know=data.get("good_to_know", []),
                executive_summary=data.get("executive_summary", ""),
            )

    # ------------------------------------------------------------------
    # Layer 3 — Report Advisor
    # ------------------------------------------------------------------

    def advise_report(
        self,
        schema_data: Dict[str, Any],
        quality_data: Dict[str, Any],
        pii_data: Dict[str, Any],
        relationships: Dict[str, Any],
        kpis: List[Dict[str, Any]],
        industry: Dict[str, Any],
        insights: Dict[str, Any] | None = None,
        type_findings: Dict[str, Any] | None = None,
    ) -> ReportAdvisorResult:
        """Run L3 Report Advisor to determine optimal report layout."""
        if not self._enabled or not self._layers.get("report_advisor"):
            return ReportAdvisorResult(skipped=True, reason="llm_disabled")

        context = build_report_context(
            schema_data, quality_data, pii_data,
            relationships, kpis, industry, insights, type_findings,
        )
        messages = build_report_advisor_messages(context)
        raw = self._call_llm("report_advisor", messages)
        if raw is None:
            return ReportAdvisorResult(skipped=True, reason="llm_call_failed")

        data = self._parse_json(raw)
        if data is None:
            return ReportAdvisorResult(skipped=True, reason="invalid_json")

        try:
            return ReportAdvisorResult(
                layout={
                    "hero_metric": data.get("hero_metric", {}),
                    "section_order": data.get("section_order", []),
                    "sections": data.get("sections", {}),
                    "executive_summary": data.get("sections", {}).get(
                        "executive_summary", {}
                    ).get("narrative", ""),
                },
                generation_notes=data.get("generation_notes", ""),
            )
        except Exception as exc:
            logger.warning("Report advisor result validation failed: {}", exc)
            return ReportAdvisorResult(skipped=True, reason=f"validation_error: {exc}")

    # ------------------------------------------------------------------
    # Usage
    # ------------------------------------------------------------------

    @property
    def enabled(self) -> bool:
        return self._enabled

    @property
    def cost_tracker(self) -> CostTracker:
        return self._cost

    def save_usage(self, path: Path) -> None:
        """Save LLM usage report."""
        self._cost.save(path)

    def usage_dict(self) -> Dict[str, Any]:
        """Return usage as a dict for embedding in report_data."""
        return self._cost.to_dict()
