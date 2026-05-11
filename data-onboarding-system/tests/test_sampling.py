"""Unit tests for sampling strategy logic."""

import pandas as pd

from app.config import SamplingConfig
from app.ingestion.sampling import SamplingStrategy


class StubConnector:
    """Simple connector stub for sampling tests."""

    def __init__(self):
        self.last_query = None
        self.sample_call = None

    def execute_query(self, query):
        self.last_query = query
        return pd.DataFrame([{"id": 1}, {"id": 2}])

    def sample_table(self, table, sample_rate, max_rows, schema):
        self.sample_call = {
            "table": table,
            "sample_rate": sample_rate,
            "max_rows": max_rows,
            "schema": schema,
        }
        return pd.DataFrame([{"id": 10}])


def test_determine_strategy_by_table_size():
    config = SamplingConfig(
        enabled=True,
        small_table_threshold=100,
        medium_sample_rate=0.2,
        large_sample_rate=0.05,
        max_sample_size=1000,
    )
    strategy = SamplingStrategy(StubConnector(), config)

    small = strategy.determine_strategy("small_table", 50)
    medium = strategy.determine_strategy("medium_table", 5_000)
    large = strategy.determine_strategy("large_table", 20_000_000)

    assert small["method"] == "full"
    assert small["sample_rate"] == 1.0
    assert medium["method"] == "sample"
    assert medium["sample_rate"] == 0.2
    assert large["method"] == "sample"
    assert large["sample_rate"] == 0.05


def test_extract_sample_full_scan_uses_execute_query():
    connector = StubConnector()
    strategy = SamplingStrategy(connector, SamplingConfig(enabled=True))
    scan = {"method": "full", "sample_rate": 1.0, "reason": "test"}

    data = strategy.extract_sample("customers", scan)

    assert not data.empty
    assert connector.last_query == 'SELECT * FROM "public"."customers"'


def test_extract_sample_sampling_uses_sample_table():
    connector = StubConnector()
    strategy = SamplingStrategy(connector, SamplingConfig(enabled=True, max_sample_size=123))
    scan = {"method": "sample", "sample_rate": 0.1, "reason": "test"}

    data = strategy.extract_sample("orders", scan)

    assert not data.empty
    assert connector.sample_call == {
        "table": "orders",
        "sample_rate": 0.1,
        "max_rows": 123,
        "schema": "public",
    }


def test_extract_sample_returns_empty_dataframe_on_error():
    class FailingConnector(StubConnector):
        def execute_query(self, query):
            raise RuntimeError("db error")

    strategy = SamplingStrategy(FailingConnector(), SamplingConfig(enabled=True))
    scan = {"method": "full", "sample_rate": 1.0, "reason": "test"}

    data = strategy.extract_sample("bad_table", scan)

    assert data.empty
