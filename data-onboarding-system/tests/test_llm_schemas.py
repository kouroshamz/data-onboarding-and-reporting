"""Tests for LLM schemas (Pydantic response models)."""

import pytest
from app.llm.schemas import (
    TypeFinding,
    TypeInspectorResult,
    Insight,
    InsightDetectorResult,
    ReportAdvisorResult,
    ReportLayout,
    HeroMetric,
    SectionDirective,
    Visualization,
)


class TestTypeInspectorSchemas:
    def test_valid_finding(self):
        f = TypeFinding(
            column="metadata",
            current_type="object",
            detected_type="json_object",
            confidence=0.95,
            severity="warning",
            recommendation="Parse JSON",
            action="parse_json",
        )
        assert f.column == "metadata"
        assert f.confidence == 0.95

    def test_confidence_bounds(self):
        with pytest.raises(Exception):
            TypeFinding(
                column="x", current_type="object",
                detected_type="json_object", confidence=1.5,
                severity="info", recommendation="test",
            )

    def test_skipped_result(self):
        r = TypeInspectorResult(skipped=True, reason="llm_disabled")
        assert r.skipped
        assert r.findings == []

    def test_result_with_findings(self):
        r = TypeInspectorResult(findings=[
            TypeFinding(
                column="status", current_type="object",
                detected_type="categorical_enum", confidence=0.9,
                severity="info", recommendation="Convert to enum",
            ),
        ])
        assert len(r.findings) == 1
        assert not r.skipped


class TestInsightSchemas:
    def test_valid_insight(self):
        i = Insight(
            category="data_scope",
            severity="warning",
            title="Narrow date range",
            detail="Only 4 days of data",
            affected_tables=["orders"],
            recommendation="Request more data",
        )
        assert i.category == "data_scope"

    def test_result_with_summary(self):
        r = InsightDetectorResult(
            insights=[
                Insight(
                    category="positive_signal",
                    severity="info",
                    title="Good quality",
                    detail="High completeness",
                ),
            ],
            good_to_know=["98% FK match rate"],
            executive_summary="Dataset is in good shape.",
        )
        assert len(r.insights) == 1
        assert len(r.good_to_know) == 1
        assert r.executive_summary != ""


class TestReportAdvisorSchemas:
    def test_hero_metric(self):
        h = HeroMetric(
            label="Quality", value="85/100",
            color="green", commentary="Good",
        )
        assert h.color == "green"

    def test_section_directive(self):
        s = SectionDirective(
            emphasis="high",
            narrative="Quality is strong.",
            visualizations=[
                Visualization(type="radar_chart", title="Components"),
            ],
        )
        assert s.emphasis == "high"
        assert len(s.visualizations) == 1

    def test_report_layout(self):
        layout = ReportLayout(
            hero_metric=HeroMetric(label="Q", value="80"),
            section_order=["executive_summary", "quality_dashboard"],
            sections={"executive_summary": SectionDirective(emphasis="high", narrative="test")},
        )
        assert len(layout.section_order) == 2

    def test_skipped_advisor_result(self):
        r = ReportAdvisorResult(skipped=True, reason="llm_disabled")
        assert r.skipped
        assert r.layout.section_order == []
