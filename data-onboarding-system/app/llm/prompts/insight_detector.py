"""L2 — Insight Detector prompt builder.

Sends a condensed dataset summary to the LLM and gets back:
  - insights (anomalies, oddities, patterns)
  - good-to-know facts
  - executive summary
"""

from __future__ import annotations

import json
from typing import Any, Dict, List

from loguru import logger


# ---------------------------------------------------------------------------
# Dataset summary builder — compresses pipeline output into ~2K tokens
# ---------------------------------------------------------------------------

def _table_summary(
    name: str,
    profile: Dict[str, Any],
    quality: Dict[str, Any],
    pii: Dict[str, Any],
) -> Dict[str, Any]:
    """One compact table summary."""
    cols_summary: List[Dict[str, Any]] = []
    for col_name, cp in profile.get("columns", {}).items():
        entry: Dict[str, Any] = {
            "name": col_name,
            "type": cp.get("type_category", cp.get("dtype", "?")),
            "null_pct": round(cp.get("null_percent", 0), 1),
            "unique_pct": round(cp.get("unique_percent", 0), 1),
        }
        # Add key stats based on type
        stats = cp.get("statistics", {})
        if stats:
            for k in ("min", "max", "mean", "std"):
                if k in stats:
                    entry[k] = stats[k]
        # Top value for categoricals
        top_vals = cp.get("top_values", [])
        if top_vals and cp.get("unique_count", 999) <= 20:
            entry["top_value"] = f"{top_vals[0]['value']} ({top_vals[0]['percent']:.0f}%)"
        cols_summary.append(entry)

    q_score = quality.get("overall_score", quality.get("quality_score", 0))
    issues: List[str] = []
    for sev, cnt in quality.get("severity_counts", {}).items():
        if cnt:
            issues.append(f"{cnt} {sev}")

    return {
        "name": name,
        "rows": profile.get("sample_size", 0),
        "columns": len(cols_summary),
        "quality_score": float(q_score),
        "completeness": profile.get("completeness_score", 0),
        "column_summaries": cols_summary[:15],  # Cap for token budget
        "pii_found": [c.get("column", "?") for c in pii.get("pii_columns", [])],
        "quality_issues": issues,
    }


def build_dataset_summary(
    schema_data: Dict[str, Any],
    profile_results: Dict[str, Any],
    quality_data: Dict[str, Any],
    pii_data: Dict[str, Any],
    relationships: Dict[str, Any],
    kpis: List[Dict[str, Any]],
    industry: Dict[str, Any],
) -> Dict[str, Any]:
    """Build the condensed dataset overview for the LLM prompt."""
    per_table_quality = quality_data.get("tables", quality_data)
    per_table_pii = pii_data.get("by_table", pii_data)

    tables: List[Dict[str, Any]] = []
    total_rows = 0
    total_cols = 0
    for tbl_name, prof in profile_results.items():
        tq = per_table_quality.get(tbl_name, {})
        tp = per_table_pii.get(tbl_name, {})
        ts = _table_summary(tbl_name, prof, tq, tp)
        tables.append(ts)
        total_rows += ts["rows"]
        total_cols += ts["columns"]

    rel_list = relationships.get("relationships", [])
    kpi_names = [k.get("name", "?") for k in kpis[:10]]

    return {
        "dataset_overview": {
            "total_tables": len(tables),
            "total_rows": total_rows,
            "total_columns": total_cols,
            "overall_quality_score": quality_data.get("overall_score", 0),
            "detected_industry": industry.get("industry", "unknown"),
            "industry_confidence": industry.get("confidence", 0),
        },
        "tables": tables,
        "relationships": [
            {
                "from": f"{r.get('table1', '?')}.{r.get('column1', '?')}",
                "to": f"{r.get('table2', '?')}.{r.get('column2', '?')}",
                "type": r.get("type", "inferred"),
            }
            for r in rel_list[:20]
        ],
        "kpi_recommendations": kpi_names,
    }


# ---------------------------------------------------------------------------
# Prompt
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You are a senior data analyst reviewing a dataset summary from an automated data onboarding pipeline.

Your job is to identify interesting patterns, anomalies, and things that a human analyst should know about this data. Focus on insights that are NOT already captured by the quality score or PII scanner.

Think about:
- Is the date range suspiciously narrow or wide?
- Are there columns with unusual distributions (e.g., 95% one value)?
- Are there sentinel/placeholder values (e.g., age=999, date=1970-01-01)?
- Does the data look like a test extract vs production data?
- Are there cross-table patterns worth noting?
- Are column names suggesting something the data doesn't show?
- Is referential integrity good or concerning?
- What's genuinely positive about this dataset?

Respond with a JSON object:
{
  "insights": [
    {
      "category": "data_scope|distribution_anomaly|sentinel_values|schema_oddity|referential_integrity|pii_risk|quality_concern|positive_signal|cross_table_pattern",
      "severity": "critical|warning|info",
      "title": "Short descriptive title",
      "detail": "2-3 sentence explanation",
      "affected_tables": ["table_name"],
      "recommendation": "What should the client or analyst do about this"
    }
  ],
  "good_to_know": [
    "Bullet-point facts that are useful but not actionable"
  ],
  "executive_summary": "2-3 paragraph non-technical summary of the dataset suitable for a client email"
}

Guidelines:
- Limit to 3-7 insights (quality over quantity)
- Limit to 3-5 good_to_know facts
- Be specific: reference actual column names, values, percentages
- Don't repeat what the quality score already says — add NEW perspective
- executive_summary should be professional and diplomatic in tone

EXAMPLE INSIGHT:
{"category": "sentinel_values", "severity": "warning", "title": "Age column contains sentinel value 999", "detail": "The age column has 14 rows (2.3%) with value 999, likely a placeholder for missing data. These were not caught by the null scanner because the field is technically populated.", "affected_tables": ["customers"], "recommendation": "Replace 999 with NULL or the population median before analysis."}"""


def build_insight_detector_messages(
    dataset_summary: Dict[str, Any],
) -> List[Dict[str, str]]:
    """Build chat messages for the insight detector call."""
    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {
            "role": "user",
            "content": json.dumps(dataset_summary, indent=2, default=str),
        },
    ]
