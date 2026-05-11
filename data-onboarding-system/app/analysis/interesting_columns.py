"""Interesting Columns Detector — Section 8 of the onboarding report.

Identifies columns that deserve special attention:
  - High variance / coefficient of variation
  - Potential correlation to revenue columns
  - Rare event indicators (low-frequency categorical values)
  - Bimodal / multimodal distributions
  - Seasonality hints in date columns
"""

from __future__ import annotations

import math
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from loguru import logger


def detect_interesting_columns(
    profile_results: Dict[str, Any],
    sample_frames: Dict[str, pd.DataFrame],
    column_classifications: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Find columns that are analytically interesting.

    Returns:
        {
            "interesting_columns": [
                {
                    "table": ..., "column": ...,
                    "reasons": [{"type": ..., "description": ..., "detail": ...}],
                    "interest_score": 0.0–1.0,
                },
            ],
            "correlations": {table: [{col_a, col_b, pearson_r}, ...]},
        }
    """
    interesting: List[Dict[str, Any]] = []
    correlations: Dict[str, List[Dict[str, Any]]] = {}

    # Identify revenue columns for correlation analysis
    revenue_cols = set()
    if column_classifications:
        for entry in column_classifications.get("by_category", {}).get("revenue", []):
            revenue_cols.add((entry["table"], entry["column"]))

    for table_name, profile in profile_results.items():
        cols = profile.get("columns", {})
        df = sample_frames.get(table_name)

        for col_name, cp in cols.items():
            reasons = []
            reasons.extend(_check_high_variance(cp))
            reasons.extend(_check_rare_events(cp))
            reasons.extend(_check_bimodal(cp, df, col_name))
            reasons.extend(_check_skewness(cp))

            if reasons:
                score = min(sum(r.get("weight", 0.3) for r in reasons), 1.0)
                interesting.append({
                    "table": table_name,
                    "column": col_name,
                    "reasons": reasons,
                    "interest_score": round(score, 2),
                })

        # Correlation matrix for numeric columns
        if df is not None and not df.empty:
            tbl_corrs = _compute_notable_correlations(df, table_name, revenue_cols)
            if tbl_corrs:
                correlations[table_name] = tbl_corrs

    # Sort by interest score descending
    interesting.sort(key=lambda x: x["interest_score"], reverse=True)

    return {
        "interesting_columns": interesting,
        "correlations": correlations,
        "count": len(interesting),
    }


def _check_high_variance(cp: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Flag columns with unusually high coefficient of variation."""
    stats = cp.get("stats", {})
    mean = stats.get("mean")
    std = stats.get("std")
    if mean is None or std is None:
        return []
    try:
        mean_f = float(mean)
        std_f = float(std)
    except (ValueError, TypeError):
        return []

    if mean_f == 0:
        return []

    cv = abs(std_f / mean_f)
    if cv > 2.0:
        return [{
            "type": "high_variance",
            "description": f"Very high coefficient of variation ({cv:.1f})",
            "detail": f"mean={mean_f:.2f}, std={std_f:.2f}",
            "weight": 0.35,
        }]
    elif cv > 1.0:
        return [{
            "type": "high_variance",
            "description": f"High coefficient of variation ({cv:.1f})",
            "detail": f"mean={mean_f:.2f}, std={std_f:.2f}",
            "weight": 0.2,
        }]
    return []


def _check_rare_events(cp: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Flag categorical columns with very rare values (potential anomaly indicators)."""
    top_values = cp.get("top_values", [])
    if not top_values or len(top_values) < 3:
        return []

    total = sum(v.get("count", 0) for v in top_values)
    if total == 0:
        return []

    # Check if the least frequent value is very rare (< 2%)
    min_count = min(v.get("count", 0) for v in top_values)
    min_pct = min_count / total * 100

    if min_pct < 1.0:
        rare_vals = [v["value"] for v in top_values if v.get("count", 0) / total * 100 < 1.0]
        return [{
            "type": "rare_events",
            "description": f"{len(rare_vals)} rare value(s) below 1% frequency",
            "detail": f"Rare: {', '.join(str(v) for v in rare_vals[:3])}",
            "weight": 0.25,
        }]
    elif min_pct < 5.0:
        return [{
            "type": "rare_events",
            "description": "Contains low-frequency values (< 5%)",
            "detail": f"Minimum frequency: {min_pct:.1f}%",
            "weight": 0.15,
        }]
    return []


def _check_bimodal(
    cp: Dict[str, Any],
    df: Optional[pd.DataFrame],
    col_name: str,
) -> List[Dict[str, Any]]:
    """Quick check for potential bimodal distribution."""
    if df is None or col_name not in df.columns:
        return []

    series = df[col_name].dropna()
    if len(series) < 30:
        return []

    # Only numeric
    if not pd.api.types.is_numeric_dtype(series):
        return []

    try:
        # Compute kurtosis — platykurtic (negative) can signal bimodality
        from scipy import stats as sp_stats
        kurtosis = float(sp_stats.kurtosis(series, nan_policy="omit"))
        if kurtosis < -1.0:
            return [{
                "type": "bimodal_hint",
                "description": f"Platykurtic distribution (kurtosis={kurtosis:.2f}), possible bimodality",
                "detail": "Consider visualising the distribution to confirm",
                "weight": 0.3,
            }]
    except Exception:
        pass

    return []


def _check_skewness(cp: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Flag highly skewed numeric columns."""
    stats = cp.get("stats", {})
    skew = stats.get("skewness")
    if skew is None:
        return []
    try:
        skew_f = float(skew)
    except (ValueError, TypeError):
        return []

    if abs(skew_f) > 3.0:
        direction = "right" if skew_f > 0 else "left"
        return [{
            "type": "high_skew",
            "description": f"Extremely {direction}-skewed (skewness={skew_f:.2f})",
            "detail": "May benefit from log or Box-Cox transformation",
            "weight": 0.25,
        }]
    elif abs(skew_f) > 1.5:
        direction = "right" if skew_f > 0 else "left"
        return [{
            "type": "moderate_skew",
            "description": f"Moderately {direction}-skewed (skewness={skew_f:.2f})",
            "detail": "Consider transformation for ML features",
            "weight": 0.15,
        }]
    return []


def _compute_notable_correlations(
    df: pd.DataFrame,
    table_name: str,
    revenue_cols: set,
) -> List[Dict[str, Any]]:
    """Find strong correlations (|r| > 0.6) among numeric columns."""
    numeric_df = df.select_dtypes(include=[np.number])
    if numeric_df.shape[1] < 2:
        return []

    try:
        corr_matrix = numeric_df.corr()
    except Exception:
        return []

    notable = []
    seen = set()

    for i, col_a in enumerate(corr_matrix.columns):
        for j, col_b in enumerate(corr_matrix.columns):
            if i >= j:
                continue
            r = corr_matrix.iloc[i, j]
            if pd.isna(r):
                continue
            pair = tuple(sorted([col_a, col_b]))
            if pair in seen:
                continue
            seen.add(pair)

            abs_r = abs(r)
            if abs_r < 0.6:
                continue

            # Boost if one is a revenue column
            involves_revenue = (
                (table_name, col_a) in revenue_cols or
                (table_name, col_b) in revenue_cols
            )

            notable.append({
                "col_a": col_a,
                "col_b": col_b,
                "pearson_r": round(float(r), 3),
                "strength": "very_strong" if abs_r > 0.8 else "strong",
                "involves_revenue": involves_revenue,
            })

    # Sort by absolute correlation descending
    notable.sort(key=lambda x: abs(x["pearson_r"]), reverse=True)
    return notable[:15]  # cap
