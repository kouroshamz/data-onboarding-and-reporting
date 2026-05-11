"""Tests for KPI detector contract compliance."""

import pytest
from pathlib import Path
import yaml

from app.config import KPIConfig
from app.kpi.detector import KPIDetector


# Helper: build schema_data with proper columns structure
def _tbl(*cols):
    return {"columns": [{"column_name": c} for c in cols]}


# ---------------------------------------------------------------------------
# Contract fields that every recommended KPI must include
# ---------------------------------------------------------------------------
CONTRACT_FIELDS = {
    "name", "description", "formula_sql", "required_fields",
    "required_tables", "grain", "dimensions", "confidence", "blocked_by",
}


# ---------------------------------------------------------------------------
# Industry detection
# ---------------------------------------------------------------------------


def test_detect_industry_respects_explicit_client_choice():
    detector = KPIDetector(KPIConfig())
    schema_data = {"tables": {"users": {}, "subscriptions": {}}}

    result = detector.detect_industry(schema_data, client_industry="saas")

    assert result["industry"] == "saas"
    assert result["method"] == "specified"
    assert result["confidence"] == 1.0


def test_detect_industry_auto_for_ecommerce_tables():
    detector = KPIDetector(KPIConfig(confidence_threshold=0.1))
    schema_data = {"tables": {"orders": {}, "customers": {}, "products": {}, "order_items": {}}}

    result = detector.detect_industry(schema_data, client_industry="auto")

    assert result["industry"] == "ecommerce"
    assert result["confidence"] > 0
    assert result["method"] == "auto_detected"


def test_detect_industry_auto_for_iot_tables():
    detector = KPIDetector(KPIConfig(confidence_threshold=0.1))
    schema_data = {"tables": {"device": {}, "telemetry": {}, "sensor": {}}}
    result = detector.detect_industry(schema_data, client_industry="auto")
    assert result["industry"] == "iot"


def test_detect_industry_falls_back_to_general():
    detector = KPIDetector(KPIConfig(confidence_threshold=0.99))
    schema_data = {"tables": {"random_table": {}}}
    result = detector.detect_industry(schema_data, client_industry="auto")
    assert result["industry"] == "general"


# ---------------------------------------------------------------------------
# KPI recommendation + contract compliance
# ---------------------------------------------------------------------------


def test_recommend_kpis_only_returns_ready_ones():
    detector = KPIDetector(KPIConfig(max_recommendations=10))
    schema_data = {"tables": {
        "orders": _tbl("order_id", "order_total", "order_date", "customer_id"),
        "customers": _tbl("customer_id", "name"),
        "order_items": _tbl("order_id", "product_id"),
        "products": _tbl("product_id", "name"),
    }}
    profiles = {}

    recommendations = detector.recommend_kpis("ecommerce", schema_data, profiles)

    assert recommendations
    assert all(k["readiness"]["is_ready"] for k in recommendations)


def test_recommended_kpis_have_all_contract_fields():
    """Every KPI must include all fields from INTERFACE_CONTRACTS_V1.md §5."""
    detector = KPIDetector(KPIConfig(max_recommendations=20))
    schema_data = {"tables": {
        "orders": _tbl("order_id", "order_total", "order_date", "customer_id"),
        "customers": _tbl("customer_id", "name"),
        "products": _tbl("product_id", "name"),
        "order_items": _tbl("order_id", "product_id"),
    }}

    recs = detector.recommend_kpis("ecommerce", schema_data)
    assert recs, "Should recommend at least one KPI"

    for kpi in recs:
        missing = CONTRACT_FIELDS - set(kpi.keys())
        assert not missing, f"KPI '{kpi.get('name')}' missing fields: {missing}"


def test_formula_sql_is_non_empty():
    detector = KPIDetector(KPIConfig(max_recommendations=20))
    schema_data = {"tables": {
        "orders": _tbl("order_id", "order_total", "order_date", "customer_id"),
        "customers": _tbl("customer_id"),
        "products": _tbl("product_id"),
        "order_items": _tbl("order_id", "product_id"),
    }}
    for kpi in detector.recommend_kpis("ecommerce", schema_data):
        assert kpi["formula_sql"].strip(), f"KPI '{kpi['name']}' has empty formula_sql"


# ---------------------------------------------------------------------------
# Full pipeline
# ---------------------------------------------------------------------------


def test_run_returns_detection_and_recommendations():
    detector = KPIDetector(KPIConfig())
    schema_data = {"tables": {
        "subscriptions": _tbl("subscription_id", "mrr", "user_id"),
        "users": _tbl("user_id", "email"),
        "invoices": _tbl("invoice_id", "amount"),
    }}
    result = detector.run(schema_data, client_industry="saas")

    assert "detection" in result
    assert "recommendations" in result
    assert "summary" in result
    assert result["summary"]["industry"] == "saas"


# ---------------------------------------------------------------------------
# Template validation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("template_name", ["ecommerce", "saas", "marketing", "iot", "general"])
def test_template_has_required_structure(template_name):
    """All templates must have industry, detection_signals, and kpis."""
    path = Path("app/kpi/templates") / f"{template_name}.yaml"
    assert path.exists(), f"Template {template_name}.yaml missing"

    with open(path) as f:
        data = yaml.safe_load(f)

    assert data["industry"] == template_name
    assert "detection_signals" in data
    assert "kpis" in data
    assert len(data["kpis"]) >= 5, f"{template_name} should have ≥5 KPIs"


@pytest.mark.parametrize("template_name", ["ecommerce", "saas", "marketing", "iot"])
def test_template_kpis_have_contract_fields(template_name):
    """All non-general template KPIs must include contract-required fields."""
    path = Path("app/kpi/templates") / f"{template_name}.yaml"
    with open(path) as f:
        data = yaml.safe_load(f)

    required = {"formula_sql", "required_fields", "required_tables", "grain", "dimensions", "blocked_by"}
    for kpi in data["kpis"]:
        missing = required - set(kpi.keys())
        assert not missing, f"{template_name} KPI '{kpi['name']}' missing: {missing}"
