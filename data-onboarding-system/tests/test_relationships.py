"""Unit tests for relationship inference."""

from app.analysis.relationships import RelationshipInferencer
from app.config import AnalysisConfig


def test_infer_relationships_detects_expected_joins():
    inferencer = RelationshipInferencer(connector=object(), config=AnalysisConfig())

    schema_data = {
        "tables": {
            "customers": {
                "row_count": 4,
                "columns": [{"column_name": "customer_id", "data_type": "integer"}],
            },
            "orders": {
                "row_count": 5,
                "columns": [
                    {"column_name": "order_id", "data_type": "integer"},
                    {"column_name": "customer_id", "data_type": "integer"},
                ],
            },
            "order_items": {
                "row_count": 10,
                "columns": [
                    {"column_name": "order_id", "data_type": "integer"},
                    {"column_name": "product_id", "data_type": "integer"},
                ],
            },
        }
    }
    profiles = {
        "customers": {"columns": {"customer_id": {"unique_percent": 100, "null_percent": 0}}},
        "orders": {
            "columns": {
                "order_id": {"unique_percent": 100, "null_percent": 0},
                "customer_id": {"unique_percent": 80, "null_percent": 0},
            }
        },
        "order_items": {
            "columns": {
                "order_id": {"unique_percent": 40, "null_percent": 0},
                "product_id": {"unique_percent": 60, "null_percent": 0},
            }
        },
    }

    result = inferencer.infer_relationships(schema_data, profiles)
    normalized_pairs = {
        (frozenset([r["table1"], r["table2"]]), r.get("column", ""))
        for r in result["relationships"]
    }

    assert (frozenset(["customers", "orders"]), "customer_id") in normalized_pairs
    assert (frozenset(["order_items", "orders"]), "order_id") in normalized_pairs
    assert len(result["join_paths"]) >= 1


def test_candidate_primary_key_detection_uses_uniqueness_and_nulls():
    inferencer = RelationshipInferencer(connector=object(), config=AnalysisConfig())
    profile = {
        "columns": {
            "id": {"unique_percent": 100, "null_percent": 0},
            "session_id": {"unique_percent": 96, "null_percent": 1},
            "name": {"unique_percent": 10, "null_percent": 0},
        }
    }

    candidates = inferencer._find_candidate_keys("events", profile)  # pylint: disable=protected-access
    by_col = {c["column"]: c for c in candidates}

    assert "id" in by_col
    assert by_col["id"]["confidence"] == "high"
    assert "session_id" in by_col
    assert by_col["session_id"]["confidence"] == "medium"
