"""Pydantic models for all LLM layer responses."""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

try:
    from pydantic import BaseModel, Field
except ImportError:
    from pydantic import BaseModel, Field  # type: ignore


# =========================================================================
# Layer 1 — Type Inspector
# =========================================================================

class TypeFinding(BaseModel):
    """A single type-mismatch finding."""
    column: str
    current_type: str
    detected_type: Literal[
        "json_object", "json_array", "csv_list", "numeric_as_string",
        "date_as_string", "boolean_as_string", "categorical_enum",
        "structured_id", "free_text", "html_xml", "base64_encoded",
    ]
    confidence: float = Field(ge=0, le=1)
    severity: Literal["critical", "warning", "info"]
    recommendation: str
    action: str = ""
    details: Dict[str, Any] = {}


class TypeInspectorResult(BaseModel):
    """Complete output of the Type Inspector layer."""
    findings: List[TypeFinding] = []
    skipped: bool = False
    reason: str = ""


# =========================================================================
# Layer 2 — Insight Detector
# =========================================================================

class Insight(BaseModel):
    """A single insight about the dataset."""
    category: Literal[
        "data_scope", "distribution_anomaly", "sentinel_values",
        "schema_oddity", "referential_integrity", "pii_risk",
        "quality_concern", "positive_signal", "cross_table_pattern",
    ]
    severity: Literal["critical", "warning", "info"]
    title: str
    detail: str
    affected_tables: List[str] = []
    recommendation: str = ""


class InsightDetectorResult(BaseModel):
    """Complete output of the Insight Detector layer."""
    insights: List[Insight] = []
    good_to_know: List[str] = []
    executive_summary: str = ""
    skipped: bool = False
    reason: str = ""


# =========================================================================
# Layer 3 — Report Advisor
# =========================================================================

class Visualization(BaseModel):
    """A chart / table recommendation for a report section."""
    type: Literal[
        "radar_chart", "bar_chart", "horizontal_bar", "gauge",
        "heatmap", "table", "status_table", "network_graph",
        "callout_cards", "histogram", "box_plot",
    ]
    data_key: str = ""
    title: str = ""
    description: str = ""


class SectionDirective(BaseModel):
    """How a single report section should be rendered."""
    emphasis: Literal["high", "medium", "low"] = "medium"
    narrative: str = ""
    visualizations: List[Visualization] = []
    top_items: List[str] = []


class HeroMetric(BaseModel):
    label: str = ""
    value: str = ""
    color: str = "blue"
    commentary: str = ""


class ReportLayout(BaseModel):
    hero_metric: HeroMetric = Field(default_factory=HeroMetric)
    section_order: List[str] = []
    sections: Dict[str, SectionDirective] = {}
    executive_summary: str = ""


class ReportAdvisorResult(BaseModel):
    """Complete output of the Report Advisor layer."""
    layout: ReportLayout = Field(default_factory=ReportLayout)
    generation_notes: str = ""
    skipped: bool = False
    reason: str = ""
