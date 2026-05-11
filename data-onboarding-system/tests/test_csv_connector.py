"""Tests for connector factory and CSV connector."""

import pytest
import pandas as pd
from pathlib import Path

from app.connectors import create_connector
from app.connectors.base import BaseConnector, AssetRef, SchemaInfo, ColumnInfo, ConnectionStatus
from app.connectors.csv_connector import CSVConnector
from app.config import ConnectionConfig


# ---------------------------------------------------------------------------
# Factory tests
# ---------------------------------------------------------------------------


def test_create_connector_postgresql():
    cfg = ConnectionConfig(type="postgresql", host="localhost", port=5432, database="db", username="u", password="p")
    conn = create_connector(cfg)
    from app.connectors.postgres import PostgreSQLConnector
    assert isinstance(conn, PostgreSQLConnector)


def test_create_connector_csv():
    cfg = ConnectionConfig(type="csv", host="/tmp")
    conn = create_connector(cfg)
    assert isinstance(conn, CSVConnector)


def test_create_connector_unsupported():
    cfg = ConnectionConfig(type="mssql", host="localhost")
    with pytest.raises(NotImplementedError, match="not supported"):
        create_connector(cfg)


# ---------------------------------------------------------------------------
# CSV connector tests
# ---------------------------------------------------------------------------


@pytest.fixture
def csv_dir(tmp_path):
    """Create a temp directory with sample CSV files."""
    (tmp_path / "users.csv").write_text("id,name,email\n1,Alice,alice@example.com\n2,Bob,bob@example.com\n")
    (tmp_path / "orders.tsv").write_text("order_id\tuser_id\ttotal\n101\t1\t99.5\n102\t2\t45.0\n103\t1\t12.0\n")
    return tmp_path


@pytest.fixture
def csv_connector(csv_dir):
    cfg = ConnectionConfig(type="csv", host=str(csv_dir))
    return CSVConnector(cfg)


def test_csv_test_connection(csv_connector):
    status = csv_connector.test_connection()
    assert isinstance(status, ConnectionStatus)
    assert status.ok is True


def test_csv_list_assets(csv_connector):
    assets = csv_connector.list_assets()
    names = [a.name for a in assets]
    # CSVConnector may strip extensions; check by substring
    assert any("users" in n for n in names)
    assert any("orders" in n for n in names)
    assert all(isinstance(a, AssetRef) for a in assets)


def test_csv_get_schema(csv_connector):
    assets = csv_connector.list_assets()
    users_asset = [a for a in assets if "users" in a.name][0]
    schema = csv_connector.get_schema(users_asset)
    assert isinstance(schema, SchemaInfo)
    col_names = [c.name for c in schema.columns]
    assert "id" in col_names
    assert "name" in col_names
    assert "email" in col_names


def test_csv_sample(csv_connector):
    assets = csv_connector.list_assets()
    users_asset = [a for a in assets if "users" in a.name][0]
    df = csv_connector.sample(users_asset, n=100)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert "id" in df.columns


def test_csv_estimate_row_count(csv_connector):
    assets = csv_connector.list_assets()
    users_asset = [a for a in assets if "users" in a.name][0]
    count = csv_connector.estimate_row_count(users_asset)
    assert count == 2


def test_csv_legacy_compat(csv_connector):
    """Test backward-compatible legacy methods."""
    tables = csv_connector.get_table_list()
    assert any("users" in t for t in tables)

    # Use the actual table name from the list
    users_table = [t for t in tables if "users" in t][0]
    df = csv_connector.execute_query(users_table)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2


# ---------------------------------------------------------------------------
# Base connector contract tests
# ---------------------------------------------------------------------------


def test_asset_ref_identifier():
    ref = AssetRef(source_id="s", asset_type="table", name="users", namespace="public")
    assert ref.identifier == "public.users"

    ref2 = AssetRef(source_id="s", asset_type="file", name="data.csv")
    assert ref2.identifier == "data.csv"


def test_connection_status_defaults():
    status = ConnectionStatus(ok=True)
    assert status.error is None
    assert status.auth_type == "password"
