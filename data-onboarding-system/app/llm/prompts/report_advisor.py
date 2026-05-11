"""L3 — Report Advisor prompt builder.

Decides:
  - Which section should lead the report (hero metric)
  - Section ordering (what matters most for THIS dataset)
  - What narrative to write for each section
  - Which visualizations suit each section

Does NOT decide visual styling — that's handled by the report template.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List

from loguru import logger


# ---------------------------------------------------------------------------
# Build compact report context
# ---------------------------------------------------------------------------

def build_report_context(
    schema_data: Dict[str, Any],
    quality_data: Dict[str, Any],
    pii_data: Dict[str, Any],
    relationships: Dict[str, Any],
    kpis: List[Dict[str, Any]],
    industry: Dict[str, Any],
    insights: Dict[str, Any] | None = None,
    type_findings: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Compact report context for the advisor LLM."""
    pii_summary = pii_data.get("summary", pii_data)
    tables = quality_data.get("tables", {})

    quality_overview = {
        "overall_score": quality_data.get("overall_score", 0),
        "components": quality_data.get("components", {}),
        "table_count": len(tables),
        "weakest_table": None,
    }
    if tables:
        worst = min(tables.items(), key=lambda x: x[1].get("overall_score", x[1].get("quality_score", 0)))
        quality_overview["weakest_table"] = {
            "name": worst[0],
            "score": worst[1].get("overall_score", worst[1].get("quality_score", 0)),
        }

    kpi_overview = {
        "total": len(kpis),
        "ready": sum(1 for k in kpis if k.get("status") == "ready"),
        "partial": sum(1 for k in kpis if k.get("status") == "partial"),
        "top_kpis": [k.get("name", "?") for k in kpis[:5]],
    }

    pii_overview = {
        "has_pii": pii_summary.get("has_pii", False),
        "tables_with_pii": pii_summary.get("tables_with_pii", 0),
        "total_pii_columns": pii_summary.get("total_pii_columns", 0),
        "risk_score": pii_summary.get("risk_score", "none"),
    }

    context: Dict[str, Any] = {
        "industry": industry.get("industry", "unknown"),
        "industry_confidence": industry.get("confidence", 0),
        "table_count": schema_data.get("table_count", 0),
        "quality": quality_overview,
        "pii": pii_overview,
        "kpis": kpi_overview,
        "relationships_count": len(relationships.get("relationships", [])),
    }

    if insights:
        context["insight_count"] = len(insights.get("insights", []))
        context["insight_severities"] = {}
        for ins in insights.get("insights", []):
            sev = ins.get("severity", "info")
            context["insight_severities"][sev] = context["insight_severities"].get(sev, 0) + 1

    if type_findings:
        all_findings = []
        for tbl_findings in type_findings.values():
            if isinstance(tbl_findings, dict):
                all_findings.extend(tbl_findings.get("findings", []))
        context["type_finding_count"] = len(all_findings)

    return context


# ---------------------------------------------------------------------------
# Prompt
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You are a report layout advisor for automated data onboarding reports.
Given a summary of dataset analysis results, decide how to structure the report for maximum impact.

Available sections:
- executive_summary: Overview for non-technical stakeholders
- quality_dashboard: Data quality scores and component breakdown
- pii_warnings: PII detection results and risk assessment
- key_insights: AI-detected anomalies and observations (only if insights exist)
- type_findings: Hidden/misclassified data types (only if findings exist)
- kpi_recommendations: Recommended KPIs with readiness status
- schema_overview: Table and column inventory
- relationships: Foreign key and inferred relationships
- appendix: Detailed per-table statistics

Rules:
1. Always include executive_summary first
2. Put the most important section second (what the client needs to see)
3. Include only sections that have meaningful content
4. If PII risk is "high" or "medium", prioritize pii_warnings
5. If quality score < 60, prioritize quality_dashboard
6. If quality score > 85, lead with positive kpi_recommendations

For each section, write a 1-2 sentence narrative that contextualizes the data.

Respond with JSON:
{
  "hero_metric": {
    "label": "Main metric label (e.g., 'Overall Data Quality')",
    "value": "The value (e.g., '72.3 / 100')",
    "color": "green|amber|red",
    "commentary": "One sentence about what this means"
  },
  "section_order": ["executive_summary", "quality_dashboard", ...],
  "sections": {
    "executive_summary": {
      "emphasis": "high|medium|low",
      "narrative": "2-3 paragraph executive summary text"
    },
    "quality_dashboard": {
      "emphasis": "high|medium|low",
      "narrative": "1-2 sentence context"
    }
  },
  "generation_notes": "Brief explanation of why you chose this layout"
}

Keep narratives factual and professional. Reference actual numbers from the data."""


def build_report_advisor_messages(
    report_context: Dict[str, Any],
) -> List[Dict[str, str]]:
    """Build chat messages for the report advisor call."""
    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {
            "role": "user",
            "content": json.dumps(report_context, indent=2, default=str),
        },
    ]
