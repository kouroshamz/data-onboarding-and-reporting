"""L1 — Type Inspector prompt builder and heuristic pre-filter.

Scans string/object columns for hidden types:
  - JSON objects/arrays stored as text
  - Numbers stored as strings
  - Dates stored as strings
  - Low-cardinality categoricals
  - Structured identifiers (SKU, UUID, etc.)
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional

import pandas as pd
from loguru import logger


# ---------------------------------------------------------------------------
# Heuristic pre-filter — keeps LLM calls minimal
# ---------------------------------------------------------------------------

def needs_llm_inspection(
    col_profile: Dict[str, Any],
    sample_values: List[Any],
) -> bool:
    """Return True if this string column looks suspicious enough
    to warrant an LLM opinion.  Cheap regex / statistics check."""

    vals = [str(v) for v in sample_values[:20] if v is not None and str(v).strip()]
    if not vals:
        return False

    # 1. JSON-like: starts with { or [
    if any(v.strip().startswith(("{", "[")) for v in vals):
        return True

    # 2. Numeric-as-string: most values parse as float
    numeric_hits = 0
    for v in vals:
        try:
            float(v.replace(",", ""))
            numeric_hits += 1
        except (ValueError, TypeError):
            pass
    if numeric_hits >= len(vals) * 0.8:
        return True

    # 3. Very low cardinality → might be categorical/enum
    unique_pct = col_profile.get("unique_percent", 100)
    if unique_pct < 2 and col_profile.get("unique_count", 999) <= 20:
        return True

    # 4. Date-like patterns
    date_re = re.compile(r"\d{4}[-/]\d{1,2}[-/]\d{1,2}")
    if sum(1 for v in vals if date_re.search(v)) >= len(vals) * 0.5:
        return True

    # 5. Boolean-as-string
    bool_vals = {"true", "false", "yes", "no", "1", "0", "t", "f", "y", "n"}
    if all(v.strip().lower() in bool_vals for v in vals):
        return True

    # 6. Very long strings (possible HTML, XML, base64)
    avg_len = col_profile.get("patterns", {}).get("avg_length", 0)
    if avg_len > 500:
        return True

    return False


# ---------------------------------------------------------------------------
# Build compact column summary for the LLM
# ---------------------------------------------------------------------------

def _build_column_summary(
    col_name: str,
    col_profile: Dict[str, Any],
    sample_data: pd.Series,
) -> Dict[str, Any]:
    """Build a token-efficient column summary for the prompt."""
    clean = sample_data.dropna()
    samples = [str(v) for v in clean.head(5).tolist()]

    return {
        "name": col_name,
        "dtype": col_profile.get("dtype", "object"),
        "null_pct": round(col_profile.get("null_percent", 0), 1),
        "unique_count": col_profile.get("unique_count", 0),
        "unique_pct": round(col_profile.get("unique_percent", 0), 1),
        "min_len": col_profile.get("patterns", {}).get("min_length", 0),
        "max_len": col_profile.get("patterns", {}).get("max_length", 0),
        "avg_len": round(col_profile.get("patterns", {}).get("avg_length", 0), 1),
        "sample_values": samples,
    }


# ---------------------------------------------------------------------------
# Build messages for the LLM
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You are a senior data engineer reviewing string/text columns from a database.
Your job is to detect columns whose actual data type is different from how they are stored.

For each column provided, analyze the sample values and statistics to determine if it contains:
- JSON objects or arrays
- Numbers stored as strings
- Dates stored as strings
- Boolean values stored as strings
- Categorical/enum values (very few unique values)
- Structured identifiers (SKU codes, UUIDs, postal codes)
- HTML, XML, or base64-encoded content
- Comma-separated lists

If a column is genuinely free-text (names, descriptions, addresses), classify it as "free_text".

Respond with a JSON object:
{
  "findings": [
    {
      "column": "column_name",
      "current_type": "object",
      "detected_type": "json_object|json_array|csv_list|numeric_as_string|date_as_string|boolean_as_string|categorical_enum|structured_id|free_text|html_xml|base64_encoded",
      "confidence": 0.95,
      "severity": "critical|warning|info",
      "recommendation": "Brief actionable recommendation",
      "action": "parse_json|convert_numeric|convert_date|convert_boolean|convert_categorical|none",
      "details": {}
    }
  ]
}

Severity guide:
- critical: data is definitely misclassified and will cause analysis errors
- warning: likely misclassified, worth converting
- info: interesting observation but acceptable as-is

Only include findings where you have confidence >= 0.6.
Do NOT include free_text columns unless they contain embedded structured data.

EXAMPLE INPUT:
{"table": "orders", "columns": [{"name": "order_total", "dtype": "object", "null_pct": 0, "unique_count": 500, "unique_pct": 50, "min_len": 3, "max_len": 8, "avg_len": 5.2, "sample_values": ["12.99", "3.50", "149.00", "0.99", "25.00"]}]}

EXAMPLE OUTPUT:
{"findings": [{"column": "order_total", "current_type": "object", "detected_type": "numeric_as_string", "confidence": 0.98, "severity": "critical", "recommendation": "Convert to DECIMAL(10,2). All sampled values parse as float — storing as text prevents aggregation.", "action": "convert_numeric", "details": {}}]}"""


def build_type_inspector_messages(
    table_name: str,
    columns: List[Dict[str, Any]],
) -> List[Dict[str, str]]:
    """Build chat messages for the type inspector call."""
    user_content = json.dumps(
        {"table": table_name, "columns": columns},
        indent=2,
        default=str,
    )
    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_content},
    ]


# ---------------------------------------------------------------------------
# Collect columns that need inspection from a profiled table
# ---------------------------------------------------------------------------

def collect_suspicious_columns(
    table_name: str,
    profile: Dict[str, Any],
    sample_data: pd.DataFrame,
) -> List[Dict[str, Any]]:
    """Return compact summaries for string columns that pass the heuristic filter."""
    suspicious: List[Dict[str, Any]] = []

    for col_name, col_profile in profile.get("columns", {}).items():
        type_cat = col_profile.get("type_category", "")
        if type_cat not in ("string", ""):
            # Only inspect string / object columns
            if col_profile.get("dtype", "") not in ("object", "str", "string"):
                continue

        if col_name not in sample_data.columns:
            continue

        col_data = sample_data[col_name]
        sample_vals = col_data.dropna().head(20).tolist()

        if needs_llm_inspection(col_profile, sample_vals):
            summary = _build_column_summary(col_name, col_profile, col_data)
            suspicious.append(summary)

    if suspicious:
        logger.info(
            "Table {}: {}/{} string columns flagged for LLM type inspection",
            table_name, len(suspicious), len(profile.get("columns", {})),
        )
    return suspicious
