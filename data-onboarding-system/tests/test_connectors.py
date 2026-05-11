"""Unit tests for connector safety behavior."""

import pandas as pd

from app.connectors.postgres import PostgreSQLConnector
from app.config import ConnectionConfig


def test_row_count_falls_back_to_exact_count_when_estimate_is_negative():
    connector = PostgreSQLConnector(
        ConnectionConfig(
            type="postgresql",
            host="localhost",
            port=5432,
            database="db",
            username="u",
            password="p",
        )
    )

    calls = []

    def fake_execute(query, params=None):
        calls.append((query, params))
        if "reltuples" in query:
            return pd.DataFrame([{"estimate": -1}])
        return pd.DataFrame([{"count": 42}])

    connector.execute_query = fake_execute  # type: ignore[assignment]

    row_count = connector.get_table_row_count("orders")

    assert row_count == 42
    assert len(calls) == 2
    assert "reltuples" in calls[0][0]
    assert "COUNT(*)" in calls[1][0]
