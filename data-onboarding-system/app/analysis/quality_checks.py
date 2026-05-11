"""Data quality checks with 5-component weighted scoring.

Implements the Quality Score Contract from INTERFACE_CONTRACTS_V1.md §6:
  - missingness  (30 pts)
  - validity     (30 pts)
  - uniqueness   (20 pts)
  - freshness    (10 pts)
  - integrity    (10 pts)

Output shape:
  {
    "overall_score": 0-100,
    "quality_score": 0-100,        # backward compat alias
    "weights": { ... },
    "components": { ... },
    "severity_counts": { ... },
    "checks": [ ... ],
    "severity_summary": { ... },   # backward compat alias
  }
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from loguru import logger


# Default weights (must sum to 100)
DEFAULT_WEIGHTS: Dict[str, int] = {
    "missingness": 30,
    "validity": 30,
    "uniqueness": 20,
    "freshness": 10,
    "integrity": 10,
}


class QualityChecker:
    """Evaluate data quality for a single table and emit a weighted score."""

    def __init__(self, connector=None, config=None):
        self.connector = connector
        self.config = config
        self._max_null = getattr(config, "max_null_percent", 50)
        self._min_fresh_days = getattr(config, "min_freshness_days", 90)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def check_table_quality(
        self,
        table: str,
        profile: Dict[str, Any],
        sample_data: pd.DataFrame,
        schema: str = "public",
    ) -> Dict[str, Any]:
        """Run all quality checks and return a contract-compliant payload."""

        checks: List[Dict[str, Any]] = []

        # 1. Missingness
        miss = self._check_missingness(profile, sample_data)
        checks.extend(miss["checks"])

        # 2. Validity
        valid = self._check_validity(sample_data, profile)
        checks.extend(valid["checks"])

        # 3. Uniqueness
        uniq = self._check_uniqueness(table, sample_data, profile)
        checks.extend(uniq["checks"])

        # 4. Freshness
        fresh = self._check_freshness(sample_data)
        checks.extend(fresh["checks"])

        # 5. Integrity
        integ = self._check_integrity(sample_data, profile)
        checks.extend(integ["checks"])

        components = {
            "missingness": miss["score"],
            "validity": valid["score"],
            "uniqueness": uniq["score"],
            "freshness": fresh["score"],
            "integrity": integ["score"],
        }

        overall = self._calculate_weighted_score(components, DEFAULT_WEIGHTS)
        severity_counts = self._count_severities(checks)

        return {
            # Contract-required fields
            "overall_score": round(overall, 1),
            "weights": dict(DEFAULT_WEIGHTS),
            "components": components,
            "severity_counts": severity_counts,
            # Backward-compat fields used by existing tests / CLI
            "quality_score": round(overall, 1),
            "checks": checks,
            "severity_summary": severity_counts,
            "completeness_score": miss["score"],
        }

    # ------------------------------------------------------------------
    # Component scorers (each returns {"score": 0-100, "checks": [...]})
    # ------------------------------------------------------------------

    def _check_missingness(self, profile: Dict, sample: pd.DataFrame) -> Dict:
        """Score based on null/missing percentages across columns."""
        checks: List[Dict] = []
        scores: List[float] = []

        columns = profile.get("columns", {})
        if not columns and not sample.empty:
            # Fallback when profile has no column data yet
            for col in sample.columns:
                null_pct = float(sample[col].isna().mean() * 100)
                col_score = max(0, 100 - null_pct)
                scores.append(col_score)
                status = "pass" if null_pct <= self._max_null else ("warning" if null_pct <= 75 else "error")
                severity = self._status_to_severity(status, "missingness")
                checks.append({
                    "check": "missingness",
                    "column": col,
                    "metric": "null_percent",
                    "value": round(null_pct, 2),
                    "status": status,
                    "severity": severity,
                    "message": f"{col}: {null_pct:.1f}% null",
                })
        else:
            for col_name, col_info in columns.items():
                null_pct = col_info.get("null_percent", 0)
                col_score = max(0, 100 - null_pct)
                scores.append(col_score)
                status = "pass" if null_pct <= self._max_null else ("warning" if null_pct <= 75 else "error")
                severity = self._status_to_severity(status, "missingness")
                checks.append({
                    "check": "missingness",
                    "column": col_name,
                    "metric": "null_percent",
                    "value": round(null_pct, 2),
                    "status": status,
                    "severity": severity,
                    "message": f"{col_name}: {null_pct:.1f}% null",
                })

        score = float(np.mean(scores)) if scores else 100.0
        return {"score": round(score, 1), "checks": checks}

    def _check_validity(self, sample: pd.DataFrame, profile: Dict) -> Dict:
        """Score based on type consistency and rule pass rates."""
        checks: List[Dict] = []
        scores: List[float] = []

        columns = profile.get("columns", {})
        for col in sample.columns:
            col_info = columns.get(col, {})
            inferred = col_info.get("inferred_type", str(sample[col].dtype))
            col_score = 100.0
            issues: List[str] = []

            # Negative values in expected-positive columns
            if sample[col].dtype in ("int64", "float64"):
                if col_info.get("inferred_type", "") in ("numeric", "integer", "float64", "int64"):
                    neg_count = int((sample[col].dropna() < 0).sum())
                    if neg_count > 0:
                        neg_pct = neg_count / max(1, len(sample[col].dropna())) * 100
                        col_score -= min(neg_pct, 50)
                        issues.append(f"{neg_count} negative values")

            # Future dates
            if "datetime" in str(sample[col].dtype).lower():
                future = (sample[col].dropna() > pd.Timestamp.now()).sum()
                if future > 0:
                    fut_pct = future / max(1, len(sample[col].dropna())) * 100
                    col_score -= min(fut_pct, 50)
                    issues.append(f"{future} future dates")

            scores.append(max(col_score, 0))
            if issues:
                checks.append({
                    "check": "validity",
                    "column": col,
                    "metric": "rule_pass_rate",
                    "value": round(col_score, 2),
                    "status": "pass" if col_score >= 90 else ("warning" if col_score >= 50 else "error"),
                    "severity": self._status_to_severity(
                        "pass" if col_score >= 90 else ("warning" if col_score >= 50 else "error"),
                        "validity",
                    ),
                    "message": f"{col}: {'; '.join(issues)}",
                })

        score = float(np.mean(scores)) if scores else 100.0
        return {"score": round(score, 1), "checks": checks}

    def _check_uniqueness(self, table: str, sample: pd.DataFrame, profile: Dict) -> Dict:
        """Score based on duplicate row rate and per-column uniqueness."""
        checks: List[Dict] = []

        # Overall duplicate rate
        if not sample.empty:
            dup_count = int(sample.duplicated().sum())
            dup_pct = dup_count / max(len(sample), 1) * 100
        else:
            dup_count = 0
            dup_pct = 0.0

        dup_score = max(0, 100 - dup_pct * 5)  # 20% dups → score 0
        status = "pass" if dup_pct < 1 else ("warning" if dup_pct < 5 else "error")
        checks.append({
            "check": "uniqueness",
            "column": "__all__",
            "metric": "duplicate_rate_percent",
            "value": round(dup_pct, 2),
            "status": status,
            "severity": self._status_to_severity(status, "uniqueness"),
            "message": f"{table}: {dup_count} duplicate rows ({dup_pct:.1f}%)",
        })

        score = round(dup_score, 1)
        return {"score": score, "checks": checks}

    def _check_freshness(self, sample: pd.DataFrame) -> Dict:
        """Score based on recency of date/timestamp columns."""
        checks: List[Dict] = []
        best_score = 0.0

        date_cols = []
        for col in sample.columns:
            if "date" in col.lower() or "time" in col.lower() or "created" in col.lower() or "updated" in col.lower():
                date_cols.append(col)
            elif sample[col].dtype == "datetime64[ns]":
                date_cols.append(col)

        if not date_cols:
            # No date columns → freshness not assessable, give benefit of the doubt
            return {
                "score": 100.0,
                "checks": [{
                    "check": "freshness",
                    "column": None,
                    "metric": "days_since_update",
                    "value": None,
                    "status": "info",
                    "severity": "minor",
                    "message": "No date columns detected for freshness check",
                }],
            }

        for col in date_cols:
            try:
                dates = pd.to_datetime(sample[col], errors="coerce")
                if dates.notna().sum() == 0:
                    continue
                latest = dates.max()
                age_days = (pd.Timestamp.now() - latest).days
                # scoring: 0 days old → 100, >= min_freshness_days → 0
                col_score = max(0, 100 * (1 - age_days / max(self._min_fresh_days, 1)))
                status = "pass" if age_days <= 7 else ("warning" if age_days <= self._min_fresh_days else "error")
                checks.append({
                    "check": "freshness",
                    "column": col,
                    "metric": "days_since_update",
                    "value": age_days,
                    "status": status,
                    "severity": self._status_to_severity(status, "freshness"),
                    "message": f"{col}: last value {age_days} days ago",
                })
                best_score = max(best_score, col_score)
            except Exception:
                continue

        return {"score": round(best_score, 1) if checks else 100.0, "checks": checks}

    def _check_integrity(self, sample: pd.DataFrame, profile: Dict) -> Dict:
        """Score based on cross-column consistency rules."""
        checks: List[Dict] = []
        scores: List[float] = []

        columns = profile.get("columns", {})

        # Check for columns that look like they should reference each other
        id_cols = [c for c in sample.columns if c.endswith("_id")]
        for col in id_cols:
            if col in sample.columns:
                null_pct = float(sample[col].isna().mean() * 100)
                # FK columns with high nulls indicate integrity issues
                col_score = max(0, 100 - null_pct * 2)
                scores.append(col_score)
                if null_pct > 5:
                    checks.append({
                        "check": "integrity",
                        "column": col,
                        "metric": "fk_null_percent",
                        "value": round(null_pct, 2),
                        "status": "warning" if null_pct <= 20 else "error",
                        "severity": self._status_to_severity(
                            "warning" if null_pct <= 20 else "error", "integrity"
                        ),
                        "message": f"Potential FK column {col}: {null_pct:.1f}% null",
                    })

        # Check for negative values in quantity/amount columns
        for col in sample.columns:
            if any(kw in col.lower() for kw in ("amount", "total", "quantity", "price", "count")):
                if sample[col].dtype in ("int64", "float64"):
                    neg = int((sample[col].dropna() < 0).sum())
                    if neg > 0:
                        neg_pct = neg / max(1, len(sample[col].dropna())) * 100
                        col_score = max(0, 100 - neg_pct * 5)
                        scores.append(col_score)
                        checks.append({
                            "check": "integrity",
                            "column": col,
                            "metric": "negative_values",
                            "value": neg,
                            "status": "warning" if neg_pct < 5 else "error",
                            "severity": self._status_to_severity(
                                "warning" if neg_pct < 5 else "error", "integrity"
                            ),
                            "message": f"{col}: {neg} negative values ({neg_pct:.1f}%)",
                        })

        score = float(np.mean(scores)) if scores else 100.0
        return {"score": round(score, 1), "checks": checks}

    # ------------------------------------------------------------------
    # Scoring helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _calculate_weighted_score(
        components: Dict[str, float], weights: Dict[str, int]
    ) -> float:
        total = 0.0
        for key, weight in weights.items():
            total += components.get(key, 100.0) * (weight / 100.0)
        return total

    @staticmethod
    def _count_severities(checks: List[Dict]) -> Dict[str, int]:
        counts = {"critical": 0, "major": 0, "minor": 0}
        for c in checks:
            sev = c.get("severity", "minor")
            if sev in counts:
                counts[sev] += 1
        return counts

    @staticmethod
    def _status_to_severity(status: str, component: str) -> str:
        """Map check status + component to severity label per PROJECT_SPEC_V1.md §5."""
        if status == "error":
            # Missingness/validity errors block KPI computation → critical
            if component in ("missingness", "validity"):
                return "critical"
            return "major"
        if status == "warning":
            return "major"
        return "minor"
