"""Missing Data Strategy Recommendation — Section 9 of the onboarding report.

Per-column recommendations for handling missing data:
  - Drop (when few rows affected or column irrelevant)
  - Impute (mean/median/mode/forward-fill based on type and distribution)
  - Transform (derive a missingness indicator)
  - Cap/flag (sentinel values, outlier-driven nulls)
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


def compute_missing_strategy(
    profile_results: Dict[str, Any],
    column_classifications: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Recommend a missing data treatment for every column with nulls.

    Returns:
        {
            "strategies": [
                {
                    "table": ..., "column": ..., "null_percent": ...,
                    "issue": ..., "treatment": ..., "rationale": ...,
                    "priority": "high" | "medium" | "low",
                },
            ],
            "summary": {
                "total_columns_with_nulls": N,
                "drop_recommended": N,
                "impute_recommended": N,
                "transform_recommended": N,
                "flag_recommended": N,
                "no_action_needed": N,
            },
        }
    """
    # Build a set of business-important columns
    important_cols = set()
    if column_classifications:
        for cat_entries in column_classifications.get("by_category", {}).values():
            for e in cat_entries:
                important_cols.add((e["table"], e["column"]))

    strategies: List[Dict[str, Any]] = []
    counts = {"drop": 0, "impute": 0, "transform": 0, "flag": 0, "no_action": 0}

    for table_name, profile in profile_results.items():
        cols = profile.get("columns", {})
        n_rows = profile.get("sample_size", 0)

        for col_name, cp in cols.items():
            null_pct = cp.get("null_percent", 0)
            if null_pct == 0:
                counts["no_action"] += 1
                continue

            rec = _recommend_treatment(
                col_name, cp, null_pct, n_rows,
                is_important=(table_name, col_name) in important_cols,
            )
            rec["table"] = table_name
            rec["column"] = col_name
            rec["null_percent"] = round(null_pct, 2)
            strategies.append(rec)
            counts[rec["treatment_type"]] = counts.get(rec["treatment_type"], 0) + 1

    strategies.sort(key=lambda x: (-x["null_percent"],))

    return {
        "strategies": strategies,
        "summary": {
            "total_columns_with_nulls": len(strategies),
            "drop_recommended": counts.get("drop", 0),
            "impute_recommended": counts.get("impute", 0),
            "transform_recommended": counts.get("transform", 0),
            "flag_recommended": counts.get("flag", 0),
            "no_action_needed": counts.get("no_action", 0),
        },
    }


def _recommend_treatment(
    col_name: str,
    col_profile: Dict[str, Any],
    null_pct: float,
    n_rows: int,
    is_important: bool,
) -> Dict[str, Any]:
    """Decide treatment for a single column."""
    type_cat = col_profile.get("type_category", col_profile.get("dtype", "unknown")).lower()
    unique_count = col_profile.get("unique_count", 0)
    stats = col_profile.get("stats", {})

    # ── Very high null (> 80%) ────────────────────────────────────────────────
    if null_pct > 80:
        if is_important:
            return {
                "issue": f"{null_pct:.0f}% missing — business-critical column",
                "treatment": "Create missingness indicator + impute with domain default",
                "treatment_type": "transform",
                "rationale": (
                    "Column is classified as business-relevant but is mostly null. "
                    "Create a binary is_missing flag and impute a domain-appropriate default."
                ),
                "priority": "high",
            }
        return {
            "issue": f"{null_pct:.0f}% missing — consider dropping",
            "treatment": "Drop column (or create a missingness indicator if needed downstream)",
            "treatment_type": "drop",
            "rationale": "Over 80% null reduces analytical utility. Dropping is simplest unless missingness itself is informative.",
            "priority": "low",
        }

    # ── Moderate null (20–80%) ────────────────────────────────────────────────
    if null_pct > 20:
        if type_cat in ("numeric", "float", "float64", "int64", "int"):
            skew = stats.get("skewness")
            if skew is not None and abs(float(skew)) > 1.5:
                method = "median"
                reason = "skewed distribution"
            else:
                method = "mean"
                reason = "roughly symmetric distribution"
            return {
                "issue": f"{null_pct:.0f}% missing in numeric column",
                "treatment": f"Impute with {method} ({reason}) + add missingness flag",
                "treatment_type": "impute",
                "rationale": f"Significant nulls but column may carry signal. {method.title()} imputation preserves central tendency for {reason}.",
                "priority": "high" if is_important else "medium",
            }
        elif type_cat in ("categorical", "object", "string"):
            if unique_count <= 10:
                return {
                    "issue": f"{null_pct:.0f}% missing in low-cardinality categorical",
                    "treatment": "Impute with mode or add 'Unknown' category",
                    "treatment_type": "impute",
                    "rationale": "Low cardinality makes mode imputation reasonable. Alternatively, treat missing as its own category.",
                    "priority": "medium",
                }
            return {
                "issue": f"{null_pct:.0f}% missing in high-cardinality categorical",
                "treatment": "Create missingness flag; optionally impute with 'Unknown'",
                "treatment_type": "transform",
                "rationale": "High cardinality makes mode imputation unreliable. A missingness flag preserves the null signal.",
                "priority": "medium",
            }
        else:
            return {
                "issue": f"{null_pct:.0f}% missing ({type_cat})",
                "treatment": "Investigate root cause before imputation",
                "treatment_type": "flag",
                "rationale": "Non-standard type with significant nulls. Manual review recommended.",
                "priority": "medium",
            }

    # ── Low null (1–20%) ─────────────────────────────────────────────────────
    if null_pct > 1:
        if type_cat in ("numeric", "float", "float64", "int64", "int"):
            return {
                "issue": f"{null_pct:.1f}% missing in numeric column",
                "treatment": "Impute with median",
                "treatment_type": "impute",
                "rationale": "Low null rate — median imputation is safe and robust to outliers.",
                "priority": "low",
            }
        elif type_cat in ("categorical", "object", "string"):
            return {
                "issue": f"{null_pct:.1f}% missing in categorical column",
                "treatment": "Impute with mode",
                "treatment_type": "impute",
                "rationale": "Low null rate — mode imputation is appropriate for categorical data.",
                "priority": "low",
            }
        else:
            return {
                "issue": f"{null_pct:.1f}% missing ({type_cat})",
                "treatment": "Drop affected rows or impute with type-appropriate default",
                "treatment_type": "impute",
                "rationale": "Low null rate — row-level dropping has minimal data loss.",
                "priority": "low",
            }

    # ── Minimal null (0–1%) ───────────────────────────────────────────────────
    return {
        "issue": f"{null_pct:.2f}% missing — negligible",
        "treatment": "Drop affected rows",
        "treatment_type": "drop",
        "rationale": "Negligible null rate. Dropping rows causes minimal data loss.",
        "priority": "low",
    }
