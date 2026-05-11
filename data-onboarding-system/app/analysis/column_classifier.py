"""Business Column Classifier — Section 6 of the onboarding report.

Classifies columns into business-meaningful categories:
  - Revenue / monetary columns
  - Timestamp / date columns
  - Geographic columns
  - Status / lifecycle columns
  - Device identifiers
  - Customer identifiers
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

import numpy as np
from loguru import logger


# ── Keyword dictionaries ──────────────────────────────────────────────────────

REVENUE_KEYWORDS = {
    "revenue", "amount", "price", "cost", "total", "sales", "payment",
    "fee", "charge", "income", "profit", "margin", "discount", "tax",
    "subtotal", "balance", "spend", "budget", "invoice", "billing",
    "mrr", "arr", "arpu", "ltv", "aov",
}

TIMESTAMP_KEYWORDS = {
    "date", "time", "timestamp", "created", "updated", "modified",
    "datetime", "day", "month", "year", "period", "start", "end",
    "open", "close", "expire", "deadline", "schedule", "born",
}

GEOGRAPHIC_KEYWORDS = {
    "country", "city", "state", "region", "zip", "postal", "address",
    "lat", "latitude", "lon", "longitude", "geo", "location", "place",
    "province", "county", "district", "territory", "continent",
}

STATUS_KEYWORDS = {
    "status", "state", "stage", "phase", "lifecycle", "category",
    "type", "class", "level", "tier", "grade", "rank", "flag",
    "active", "enabled", "approved", "completed", "pending",
}

DEVICE_KEYWORDS = {
    "device", "sensor", "machine", "equipment", "asset", "serial",
    "imei", "mac", "hardware", "firmware", "model", "manufacturer",
    "iot", "gateway", "node", "beacon", "tag",
}

CUSTOMER_KEYWORDS = {
    "customer", "client", "user", "account", "member", "subscriber",
    "patient", "student", "employee", "contact", "person", "tenant",
    "buyer", "seller", "vendor", "partner",
}


_ALL_CATEGORIES = {
    "revenue": REVENUE_KEYWORDS,
    "timestamp": TIMESTAMP_KEYWORDS,
    "geographic": GEOGRAPHIC_KEYWORDS,
    "status_lifecycle": STATUS_KEYWORDS,
    "device_identifier": DEVICE_KEYWORDS,
    "customer_identifier": CUSTOMER_KEYWORDS,
}


def classify_columns(
    profile_results: Dict[str, Any],
    schema_data: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Classify columns into business categories.

    Returns:
        {
            "classifications": [
                {"table": ..., "column": ..., "category": ..., "confidence": ..., "signals": [...]},
            ],
            "by_category": {
                "revenue": [{"table": ..., "column": ...}, ...],
                ...
            },
            "summary": {"total_classified": N, "categories_found": [...]}
        }
    """
    classifications: List[Dict[str, Any]] = []

    for table_name, profile in profile_results.items():
        cols = profile.get("columns", {})
        for col_name, cp in cols.items():
            result = _classify_single_column(col_name, cp)
            if result:
                result["table"] = table_name
                result["column"] = col_name
                classifications.append(result)

    # Group by category
    by_category: Dict[str, List[Dict[str, str]]] = {}
    for c in classifications:
        cat = c["category"]
        by_category.setdefault(cat, []).append({
            "table": c["table"],
            "column": c["column"],
            "confidence": c["confidence"],
        })

    categories_found = sorted(by_category.keys())

    return {
        "classifications": classifications,
        "by_category": by_category,
        "summary": {
            "total_classified": len(classifications),
            "categories_found": categories_found,
        },
    }


def _classify_single_column(col_name: str, col_profile: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Classify a single column using keyword + statistical heuristics."""
    name_lower = col_name.lower().replace("-", "_")
    tokens = set(re.split(r"[_\s]+", name_lower))
    type_cat = col_profile.get("type_category", col_profile.get("dtype", "unknown")).lower()
    dtype = col_profile.get("dtype", "").lower()
    null_pct = col_profile.get("null_percent", 0)

    best_cat = None
    best_score = 0
    best_signals: List[str] = []

    for category, keywords in _ALL_CATEGORIES.items():
        score = 0
        signals = []

        # Keyword match (name tokens)
        matched_kws = tokens & keywords
        if matched_kws:
            score += 0.6
            signals.append(f"name_match: {', '.join(matched_kws)}")

        # Substring match (partial)
        if not matched_kws:
            for kw in keywords:
                if kw in name_lower and len(kw) >= 3:
                    score += 0.4
                    signals.append(f"substring: {kw}")
                    break

        # Type-based boost
        if category == "revenue" and type_cat in ("numeric", "float", "float64", "int64"):
            stats = col_profile.get("stats", {})
            if stats.get("min", 0) >= 0:
                score += 0.2
                signals.append("non_negative_numeric")
        elif category == "timestamp" and ("date" in dtype or "time" in dtype):
            score += 0.3
            signals.append(f"dtype: {dtype}")
        elif category in ("status_lifecycle",) and type_cat in ("categorical", "object", "string"):
            unique_count = col_profile.get("unique_count", 0)
            if 2 <= unique_count <= 20:
                score += 0.2
                signals.append(f"low_cardinality ({unique_count} unique)")
        elif category in ("customer_identifier", "device_identifier"):
            unique_pct = col_profile.get("unique_percent", 0)
            if unique_pct > 80:
                score += 0.15
                signals.append(f"high_uniqueness ({unique_pct:.0f}%)")

        if score > best_score:
            best_score = score
            best_cat = category
            best_signals = signals

    if best_score < 0.35:
        return None

    confidence = min(round(best_score, 2), 1.0)
    confidence_label = "high" if confidence >= 0.7 else "medium" if confidence >= 0.5 else "low"

    return {
        "category": best_cat,
        "confidence": confidence,
        "confidence_label": confidence_label,
        "signals": best_signals,
    }
