"""S3 connector for CSV / Parquet objects.

Implements BaseConnector using boto3.  Falls back gracefully when
boto3 is not installed.
"""

from __future__ import annotations

import io
import time
from datetime import datetime
from typing import List, Optional

import pandas as pd
from loguru import logger

from app.config import ConnectionConfig
from app.connectors.base import (
    AssetRef,
    BaseConnector,
    ColumnInfo,
    ConnectionStatus,
    SchemaInfo,
)

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:  # pragma: no cover
    boto3 = None  # type: ignore[assignment]


def _dtype_to_sql(dtype) -> str:
    name = str(dtype)
    if "int" in name:
        return "integer"
    if "float" in name:
        return "numeric"
    if "bool" in name:
        return "boolean"
    if "datetime" in name:
        return "timestamp"
    return "text"


class S3Connector(BaseConnector):
    """Read-only connector for S3 buckets containing CSV or Parquet."""

    def __init__(self, config: ConnectionConfig):
        self.config = config
        self._bucket = config.database or ""  # bucket name
        self._prefix = config.host or ""  # object prefix (folder)
        self._client = None
        self._keys: List[str] = []

    # ------------------------------------------------------------------
    # BaseConnector contract
    # ------------------------------------------------------------------

    def test_connection(self) -> ConnectionStatus:
        t0 = time.monotonic()
        if boto3 is None:
            return ConnectionStatus(ok=False, error="boto3 not installed", auth_type="iam")
        try:
            client = self._get_client()
            client.head_bucket(Bucket=self._bucket)
            latency = int((time.monotonic() - t0) * 1000)
            return ConnectionStatus(ok=True, latency_ms=latency, auth_type="iam")
        except Exception as exc:
            latency = int((time.monotonic() - t0) * 1000)
            return ConnectionStatus(ok=False, error=str(exc), latency_ms=latency, auth_type="iam")

    def list_assets(self) -> List[AssetRef]:
        self._discover()
        return [
            AssetRef(
                source_id=f"s3://{self._bucket}",
                asset_type="object",
                name=key.rsplit("/", 1)[-1].rsplit(".", 1)[0],
                namespace=self._bucket,
            )
            for key in self._keys
        ]

    def get_schema(self, asset: AssetRef) -> SchemaInfo:
        df = self.sample(asset, n=5)
        columns = [
            ColumnInfo(
                name=str(col),
                declared_type=_dtype_to_sql(df[col].dtype),
                inferred_type=_dtype_to_sql(df[col].dtype),
                nullable=bool(df[col].isna().any()),
            )
            for col in df.columns
        ]
        return SchemaInfo(columns=columns)

    def sample(self, asset: AssetRef, n: int = 10_000) -> pd.DataFrame:
        key = self._key_for(asset)
        obj = self._get_client().get_object(Bucket=self._bucket, Key=key)
        body = obj["Body"].read()
        if key.endswith(".parquet"):
            df = pd.read_parquet(io.BytesIO(body))
        elif key.endswith(".tsv"):
            df = pd.read_csv(io.BytesIO(body), sep="\t", nrows=n)
        else:
            df = pd.read_csv(io.BytesIO(body), nrows=n)
        return df.head(n)

    def estimate_row_count(self, asset: AssetRef) -> Optional[int]:
        try:
            df = self.sample(asset, n=100_000)
            return len(df)
        except Exception:
            return None

    def get_freshness(self, asset: AssetRef) -> Optional[datetime]:
        key = self._key_for(asset)
        try:
            meta = self._get_client().head_object(Bucket=self._bucket, Key=key)
            return meta["LastModified"]
        except Exception:
            return None

    # ------------------------------------------------------------------
    # Legacy compat helpers
    # ------------------------------------------------------------------

    def connect(self):
        self._discover()
        logger.info(f"S3 connector ready – {len(self._keys)} object(s) in s3://{self._bucket}/{self._prefix}")
        return self

    def get_table_list(self, schema: str = "public") -> List[str]:
        self._discover()
        return [k.rsplit("/", 1)[-1].rsplit(".", 1)[0] for k in self._keys]

    def get_table_row_count(self, table: str, schema: str = "public") -> int:
        asset = self._asset_for(table)
        return self.estimate_row_count(asset) or 0

    def get_column_info(self, table: str, schema: str = "public") -> pd.DataFrame:
        asset = self._asset_for(table)
        si = self.get_schema(asset)
        rows = [
            {"column_name": c.name, "data_type": c.declared_type,
             "is_nullable": "YES" if c.nullable else "NO",
             "column_default": None, "character_maximum_length": None,
             "numeric_precision": None, "numeric_scale": None}
            for c in si.columns
        ]
        return pd.DataFrame(rows)

    def execute_query(self, query: str, params=None) -> pd.DataFrame:
        for k in self._keys:
            stem = k.rsplit("/", 1)[-1].rsplit(".", 1)[0]
            if stem in query:
                asset = self._asset_for(stem)
                return self.sample(asset, n=100_000)
        return pd.DataFrame()

    def sample_table(self, table: str, sample_rate: float = 0.1,
                     max_rows: Optional[int] = None, schema: str = "public") -> pd.DataFrame:
        asset = self._asset_for(table)
        return self.sample(asset, n=max_rows or 10_000)

    def get_primary_keys(self, table: str, schema: str = "public") -> list:
        return []

    def get_foreign_keys(self, table: str, schema: str = "public") -> list:
        return []

    def get_indexes(self, table: str, schema: str = "public") -> list:
        return []

    def close(self):
        self._client = None

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _get_client(self):
        if self._client is None:
            if boto3 is None:
                raise ImportError("boto3 is required for S3 connector")
            self._client = boto3.client("s3")
        return self._client

    def _discover(self):
        if self._keys:
            return
        client = self._get_client()
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=self._prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith((".csv", ".tsv", ".parquet", ".json")):
                    self._keys.append(key)
        self._keys.sort()

    def _key_for(self, asset: AssetRef) -> str:
        for k in self._keys:
            stem = k.rsplit("/", 1)[-1].rsplit(".", 1)[0]
            if stem == asset.name:
                return k
        raise FileNotFoundError(f"No S3 object matching asset name: {asset.name}")

    def _asset_for(self, table: str) -> AssetRef:
        return AssetRef(source_id=f"s3://{self._bucket}", asset_type="object",
                        name=table, namespace=self._bucket)
