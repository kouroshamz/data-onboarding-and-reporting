"""Unit tests for KPI detector behavior."""

from app.config import KPIConfig
from app.kpi.detector import KPIDetector


# Helper: build table metadata with proper columns structure
def _tbl(*cols):
    return {"columns": [{"column_name": c} for c in cols]}


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
