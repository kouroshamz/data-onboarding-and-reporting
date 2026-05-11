"""MySQL and S3 connector tests — fully mocked (no real services needed)."""

import time
from datetime import datetime
from unittest.mock import MagicMock, patch, PropertyMock

import pandas as pd
import pytest

from app.config import ConnectionConfig
from app.connectors.base import AssetRef, ConnectionStatus, SchemaInfo


# =========================================================================
# MySQL Connector (mocked SQLAlchemy)
# =========================================================================

class TestMySQLConnector:

    @pytest.fixture
    def mysql_cfg(self):
        return ConnectionConfig(
            type="mysql", host="localhost", port=3306,
            database="testdb", username="root", password="secret",
        )

    @pytest.fixture
    def conn(self, mysql_cfg):
        from app.connectors.mysql_connector import MySQLConnector
        return MySQLConnector(mysql_cfg)

    @patch("app.connectors.mysql_connector.create_engine")
    def test_test_connection_success(self, mock_engine, conn):
        mock_conn = MagicMock()
        mock_engine.return_value.connect.return_value.__enter__ = lambda s: mock_conn
        mock_engine.return_value.connect.return_value.__exit__ = MagicMock(return_value=False)
        status = conn.test_connection()
        assert status.ok is True
        assert status.auth_type == "password"

    @patch("app.connectors.mysql_connector.create_engine")
    def test_test_connection_failure(self, mock_engine, conn):
        mock_engine.return_value.connect.side_effect = Exception("Connection refused")
        status = conn.test_connection()
        assert status.ok is False
        assert "Connection refused" in status.error

    @patch("app.connectors.mysql_connector.create_engine")
    def test_list_assets(self, mock_engine, conn):
        # Mock the internal get_table_list to avoid needing full SQLAlchemy chain
        conn.get_table_list = MagicMock(return_value=["users", "orders", "products"])
        assets = conn.list_assets()
        assert len(assets) == 3
        names = [a.name for a in assets]
        assert "users" in names
        assert "orders" in names

    @patch("app.connectors.mysql_connector.create_engine")
    def test_get_schema(self, mock_engine, conn):
        # Mock get_column_info to return a DataFrame
        mock_df = pd.DataFrame({
            "column_name": ["id", "name", "email"],
            "data_type": ["int", "varchar", "varchar"],
            "is_nullable": ["NO", "YES", "YES"],
        })
        conn.get_column_info = MagicMock(return_value=mock_df)
        asset = AssetRef(source_id="localhost/testdb", asset_type="table",
                         name="users", namespace="testdb")
        schema = conn.get_schema(asset)
        assert isinstance(schema, SchemaInfo)
        assert len(schema.columns) == 3

    def test_close_does_not_crash(self, conn):
        conn.close()  # Should not raise


# =========================================================================
# S3 Connector (mocked boto3)
# =========================================================================

class TestS3Connector:

    @pytest.fixture
    def s3_cfg(self):
        return ConnectionConfig(
            type="s3", database="my-bucket", host="data/",
        )

    @pytest.fixture
    def conn(self, s3_cfg):
        from app.connectors.s3_connector import S3Connector
        return S3Connector(s3_cfg)

    @patch("app.connectors.s3_connector.boto3")
    def test_test_connection_success(self, mock_boto3, conn):
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        mock_client.head_bucket.return_value = {}
        status = conn.test_connection()
        assert status.ok is True
        assert status.auth_type == "iam"

    @patch("app.connectors.s3_connector.boto3")
    def test_test_connection_failure(self, mock_boto3, conn):
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        mock_client.head_bucket.side_effect = Exception("Bucket not found")
        status = conn.test_connection()
        assert status.ok is False
        assert "Bucket not found" in status.error

    def test_test_connection_no_boto3(self, conn):
        """Without boto3, should fail gracefully."""
        with patch("app.connectors.s3_connector.boto3", None):
            status = conn.test_connection()
            assert status.ok is False
            assert "boto3" in status.error.lower()

    @patch("app.connectors.s3_connector.boto3")
    def test_list_assets(self, mock_boto3, conn):
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        # Simulate paginator
        mock_paginator = MagicMock()
        mock_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {"Contents": [
                {"Key": "data/orders.csv"},
                {"Key": "data/products.tsv"},
                {"Key": "data/readme.md"},  # Should be excluded
            ]}
        ]
        conn._client = None  # Force re-create
        assets = conn.list_assets()
        # Only csv/tsv should be included
        assert len(assets) == 2
        names = [a.name for a in assets]
        assert "orders" in names
        assert "products" in names

    @patch("app.connectors.s3_connector.boto3")
    def test_sample_csv_object(self, mock_boto3, conn):
        import io
        csv_content = b"id,name,value\n1,Alice,100\n2,Bob,200\n"
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        mock_client.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=csv_content)),
        }
        # Pre-populate keys
        conn._keys = ["data/orders.csv"]
        asset = AssetRef(source_id="s3://my-bucket", asset_type="object",
                         name="orders", namespace="my-bucket")
        df = conn.sample(asset, n=10)
        assert len(df) == 2
        assert list(df.columns) == ["id", "name", "value"]

    @patch("app.connectors.s3_connector.boto3")
    def test_get_freshness(self, mock_boto3, conn):
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        last_mod = datetime(2024, 6, 15, 12, 0, 0)
        mock_client.head_object.return_value = {"LastModified": last_mod}
        conn._keys = ["data/orders.csv"]
        asset = AssetRef(source_id="s3://my-bucket", asset_type="object",
                         name="orders", namespace="my-bucket")
        freshness = conn.get_freshness(asset)
        assert freshness == last_mod

    def test_close_resets_client(self, conn):
        conn._client = MagicMock()
        conn.close()
        assert conn._client is None
