"""KPI detection and recommendation engine.

Implements the KPI Candidate Contract from INTERFACE_CONTRACTS_V1.md §5.
Every recommended KPI includes:
  name, description, formula_sql, required_fields, required_tables,
  grain, dimensions, confidence, blocked_by.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from loguru import logger


# ---------------------------------------------------------------------------
# Template loader
# ---------------------------------------------------------------------------

_TEMPLATE_DIR = Path(__file__).parent / "templates"


def _load_templates(directory: Path | None = None) -> Dict[str, Any]:
    """Load all YAML KPI templates from *directory*."""
    d = directory or _TEMPLATE_DIR
    templates: Dict[str, Any] = {}
    for p in sorted(d.glob("*.yaml")):
        try:
            with open(p) as f:
                data = yaml.safe_load(f)
            if data and "industry" in data:
                templates[data["industry"]] = data
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to load KPI template {}: {}", p, exc)
    return templates


# ---------------------------------------------------------------------------
# KPI Detector
# ---------------------------------------------------------------------------

class KPIDetector:
    """Detect industry from schema and recommend contract-compliant KPIs."""

    def __init__(self, config: Any = None, template_dir: Path | None = None):
        self.config = config
        self.templates = _load_templates(template_dir)

        # Pull settings from config object (KPIConfig) if available
        self.confidence_threshold: float = getattr(config, "confidence_threshold", 0.3)
        self.max_recommendations: int = getattr(config, "max_recommendations", 10)
        self.enabled_industries: List[str] = getattr(
            config, "industries", list(self.templates.keys())
        )

    # ------------------------------------------------------------------
    # Industry detection
    # ------------------------------------------------------------------

    def detect_industry(
        self,
        schema_data: Dict[str, Any],
        client_industry: str = "auto",
    ) -> Dict[str, Any]:
        """Detect the most likely industry for a dataset.

        Parameters
        ----------
        schema_data : dict
            Must contain ``{"tables": {"table_name": {...}, ...}}``.
        client_industry : str
            If not ``"auto"``, treat it as a hard override.

        Returns
        -------
        dict  with ``industry``, ``method``, ``confidence``, ``runner_up``.
        """
        if client_industry and client_industry.lower() not in ("auto", "", "none"):
            return {
                "industry": client_industry.lower(),
                "method": "specified",
                "confidence": 1.0,
                "runner_up": None,
            }

        table_names = {t.lower() for t in schema_data.get("tables", {})}
        column_names: set[str] = set()
        for tbl_info in schema_data.get("tables", {}).values():
            if isinstance(tbl_info, dict):
                for col in tbl_info.get("columns", []):
                    if isinstance(col, dict):
                        name = col.get("column_name") or col.get("name", "")
                        if name:
                            column_names.add(name.lower())

        scores: Dict[str, float] = {}
        for ind, tmpl in self.templates.items():
            if ind == "general":
                continue
            signals = tmpl.get("detection_signals", {})
            sig_tables = {t.lower() for t in signals.get("tables", [])}
            sig_cols = {c.lower() for c in signals.get("columns", [])}
            total = len(sig_tables) + len(sig_cols)
            if total == 0:
                continue
            hits = len(sig_tables & table_names) + len(sig_cols & column_names)
            scores[ind] = hits / total

        if not scores:
            return {"industry": "general", "method": "auto_detected", "confidence": 0.0, "runner_up": None}

        ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        best, best_score = ranked[0]
        runner_up = ranked[1] if len(ranked) > 1 else None

        if best_score < self.confidence_threshold:
            return {"industry": "general", "method": "auto_detected", "confidence": best_score, "runner_up": best}

        return {
            "industry": best,
            "method": "auto_detected",
            "confidence": round(best_score, 4),
            "runner_up": runner_up[0] if runner_up else None,
        }

    # ------------------------------------------------------------------
    # KPI recommendation
    # ------------------------------------------------------------------

    def recommend_kpis(
        self,
        industry: str,
        schema_data: Dict[str, Any],
        profiles: Dict[str, Any] | None = None,
    ) -> List[Dict[str, Any]]:
        """Return recommended KPIs for *industry* filtered by data readiness.

        Each returned KPI conforms to the KPI Candidate Contract
        (INTERFACE_CONTRACTS_V1.md §5).
        """
        tmpl = self.templates.get(industry)
        if tmpl is None:
            tmpl = self.templates.get("general")
        if tmpl is None:
            logger.warning("No KPI template found for industry '{}'", industry)
            return []

        table_names = {t.lower() for t in schema_data.get("tables", {})}
        column_names: set[str] = set()
        for tbl_info in schema_data.get("tables", {}).values():
            if isinstance(tbl_info, dict):
                for col in tbl_info.get("columns", []):
                    if isinstance(col, dict):
                        name = col.get("column_name") or col.get("name", "")
                        if name:
                            column_names.add(name.lower())

        results: List[Dict[str, Any]] = []
        for kpi in tmpl.get("kpis", []):
            readiness = self._assess_readiness(kpi, table_names, column_names)
            if not readiness["is_ready"]:
                continue
            candidate = self._to_contract(kpi, readiness)
            results.append(candidate)

        # Sort by priority, limit
        results.sort(key=lambda k: k.get("priority", 99))
        return results[: self.max_recommendations]

    # ------------------------------------------------------------------
    # Readiness check
    # ------------------------------------------------------------------

    @staticmethod
    def _assess_readiness(
        kpi: Dict[str, Any],
        tables: set[str],
        columns: set[str],
    ) -> Dict[str, Any]:
        """Check whether the dataset can support *kpi*."""
        req_tables = {t.lower() for t in kpi.get("required_tables", [])}
        req_fields = {f.lower() for f in kpi.get("required_fields", [])}

        # Also check legacy required_columns for IoT compat
        legacy_req = kpi.get("required_columns", {})
        legacy_any_of = {c.lower() for c in legacy_req.get("any_of", [])} if isinstance(legacy_req, dict) else set()

        missing_tables = req_tables - tables
        missing_fields = req_fields - columns

        # If required_fields is empty (general template), consider ready
        fields_ok = len(missing_fields) == 0 or len(req_fields) == 0
        tables_ok = len(missing_tables) == 0 or len(req_tables) == 0

        # Legacy columns: at least one must match
        legacy_ok = True
        if legacy_any_of:
            legacy_ok = bool(legacy_any_of & columns) or bool(legacy_any_of & tables)

        is_ready = fields_ok and tables_ok and legacy_ok

        return {
            "is_ready": is_ready,
            "missing_tables": sorted(missing_tables),
            "missing_fields": sorted(missing_fields),
        }

    # ------------------------------------------------------------------
    # Contract mapper
    # ------------------------------------------------------------------

    @staticmethod
    def _to_contract(kpi: Dict[str, Any], readiness: Dict[str, Any]) -> Dict[str, Any]:
        """Map a template KPI entry to the KPI Candidate Contract shape."""
        return {
            "name": kpi.get("name", "Unnamed KPI"),
            "description": kpi.get("description", ""),
            "category": kpi.get("category", ""),
            "priority": kpi.get("priority", 99),
            "formula_sql": kpi.get("formula_sql", kpi.get("sql_template", "")),
            "required_fields": kpi.get("required_fields", []),
            "required_tables": kpi.get("required_tables", []),
            "grain": kpi.get("grain", "unknown"),
            "dimensions": kpi.get("dimensions", []),
            "confidence": 1.0,  # If we recommend it, schema matches
            "blocked_by": kpi.get("blocked_by", []),
            "readiness": readiness,
        }

    # ------------------------------------------------------------------
    # Full pipeline helper
    # ------------------------------------------------------------------

    def run(
        self,
        schema_data: Dict[str, Any],
        profiles: Dict[str, Any] | None = None,
        client_industry: str = "auto",
    ) -> Dict[str, Any]:
        """Full detection + recommendation pipeline."""
        detection = self.detect_industry(schema_data, client_industry)
        recommendations = self.recommend_kpis(detection["industry"], schema_data, profiles)
        return {
            "detection": detection,
            "recommendations": recommendations,
            "summary": {
                "industry": detection["industry"],
                "method": detection["method"],
                "total_recommended": len(recommendations),
            },
        }
