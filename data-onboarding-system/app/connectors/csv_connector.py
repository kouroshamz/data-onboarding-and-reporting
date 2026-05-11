"""CSV / TSV / XLSX file connector.

Treats a directory (or single file) as a "database" where each file is an asset.
Implements BaseConnector (INTERFACE_CONTRACTS_V1.md §2).
"""

from __future__ import annotations

import os
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import numpy as np
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

_SUPPORTED_EXTENSIONS = {".csv", ".tsv", ".xlsx", ".xls", ".parquet", ".json"}


def _read_file(path: Path, nrows: Optional[int] = None) -> pd.DataFrame:
    """Read a tabular file into a DataFrame."""
    ext = path.suffix.lower()
    kwargs: dict = {}
    if nrows is not None:
        kwargs["nrows"] = nrows

    if ext == ".csv":
        return pd.read_csv(path, **kwargs)
    elif ext == ".tsv":
        return pd.read_csv(path, sep="\t", **kwargs)
    elif ext in (".xlsx", ".xls"):
        # openpyxl nrows not supported directly – read all then truncate
        df = pd.read_excel(path)
        if nrows is not None:
            df = df.head(nrows)
        return df
    elif ext == ".parquet":
        df = pd.read_parquet(path)
        if nrows is not None:
            df = df.head(nrows)
        return df
    elif ext == ".json":
        df = pd.read_json(path)
        if nrows is not None:
            df = df.head(nrows)
        return df
    else:
        raise ValueError(f"Unsupported file extension: {ext}")


def _dtype_to_sql(dtype) -> str:
    """Map pandas dtype to a SQL-like type name."""
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


class CSVConnector(BaseConnector):
    """Connector for local flat-file data sources (CSV, TSV, XLSX, Parquet)."""

    def __init__(self, config: ConnectionConfig):
        self.config = config
        # `host` is overloaded to mean the directory or file path
        self._root = Path(config.host or ".")
        self._files: List[Path] = []

    # ------------------------------------------------------------------
    # BaseConnector contract
    # ------------------------------------------------------------------

    def test_connection(self) -> ConnectionStatus:
        t0 = time.monotonic()
        try:
            if not self._root.exists():
                return ConnectionStatus(
                    ok=False,
                    error=f"Path does not exist: {self._root}",
                    latency_ms=0,
                    auth_type="filesystem",
                )
            self._discover_files()
            latency = int((time.monotonic() - t0) * 1000)
            return ConnectionStatus(ok=True, latency_ms=latency, auth_type="filesystem")
        except Exception as exc:
            latency = int((time.monotonic() - t0) * 1000)
            return ConnectionStatus(ok=False, error=str(exc), latency_ms=latency, auth_type="filesystem")

    def list_assets(self) -> List[AssetRef]:
        self._discover_files()
        return [
            AssetRef(
                source_id=str(self._root),
                asset_type="file",
                name=p.stem,
                namespace=str(p.parent),
            )
            for p in self._files
        ]

    def get_schema(self, asset: AssetRef) -> SchemaInfo:
        path = self._resolve_path(asset)
        df = _read_file(path, nrows=5)
        columns = []
        for col in df.columns:
            columns.append(
                ColumnInfo(
                    name=str(col),
                    declared_type=_dtype_to_sql(df[col].dtype),
                    inferred_type=_dtype_to_sql(df[col].dtype),
                    nullable=bool(df[col].isna().any()),
                )
            )
        return SchemaInfo(columns=columns)

    def sample(self, asset: AssetRef, n: int = 10_000) -> pd.DataFrame:
        path = self._resolve_path(asset)
        return _read_file(path, nrows=n)

    def estimate_row_count(self, asset: AssetRef) -> Optional[int]:
        path = self._resolve_path(asset)
        ext = path.suffix.lower()
        if ext in (".csv", ".tsv"):
            # Fast line count
            with open(path, "rb") as f:
                count = sum(1 for _ in f)
            return max(count - 1, 0)  # subtract header
        try:
            return len(_read_file(path))
        except Exception:
            return None

    def get_freshness(self, asset: AssetRef) -> Optional[datetime]:
        path = self._resolve_path(asset)
        df = _read_file(path, nrows=100)
        for col in df.columns:
            try:
                parsed = pd.to_datetime(df[col], errors="coerce")
                if parsed.notna().sum() > len(parsed) * 0.5:
                    return parsed.max().to_pydatetime()
            except Exception:
                continue
        # Fallback to file modification time
        mtime = os.path.getmtime(path)
        return datetime.fromtimestamp(mtime)

    # ------------------------------------------------------------------
    # Legacy-compat helpers (so SchemaExtractor / SamplingStrategy work)
    # ------------------------------------------------------------------

    def connect(self):
        self._discover_files()
        logger.info(f"CSV connector ready – {len(self._files)} file(s) under {self._root}")
        return self

    def execute_query(self, query: str, params=None) -> pd.DataFrame:
        """Minimal query support: SELECT * FROM "<namespace>"."<table>" """
        # Very simple parser for the legacy code paths
        q = query.strip().rstrip(";")
        for f in self._files:
            if f.stem in q:
                return _read_file(f)
        logger.warning(f"CSV connector cannot resolve query: {query}")
        return pd.DataFrame()

    def get_table_list(self, schema: str = "public") -> List[str]:
        self._discover_files()
        return [p.stem for p in self._files]

    def get_table_row_count(self, table: str, schema: str = "public") -> int:
        asset = self._asset_for(table)
        return self.estimate_row_count(asset) or 0

    def get_column_info(self, table: str, schema: str = "public") -> pd.DataFrame:
        asset = self._asset_for(table)
        si = self.get_schema(asset)
        rows = [
            {
                "column_name": c.name,
                "data_type": c.declared_type,
                "is_nullable": "YES" if c.nullable else "NO",
                "column_default": None,
                "character_maximum_length": None,
                "numeric_precision": None,
                "numeric_scale": None,
            }
            for c in si.columns
        ]
        return pd.DataFrame(rows)

    def sample_table(self, table: str, sample_rate: float = 0.1,
                     max_rows: Optional[int] = None, schema: str = "public") -> pd.DataFrame:
        asset = self._asset_for(table)
        n = max_rows or 10_000
        df = self.sample(asset, n=n)
        if sample_rate < 1.0:
            df = df.sample(frac=sample_rate, random_state=42)
        return df

    def get_primary_keys(self, table: str, schema: str = "public") -> List[str]:
        return []

    def get_foreign_keys(self, table: str, schema: str = "public") -> list:
        return []

    def get_indexes(self, table: str, schema: str = "public") -> list:
        return []

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _discover_files(self):
        if self._files:
            return
        if self._root.is_file():
            self._files = [self._root]
        elif self._root.is_dir():
            self._files = sorted(
                p for p in self._root.iterdir()
                if p.is_file() and p.suffix.lower() in _SUPPORTED_EXTENSIONS
            )
        else:
            self._files = []

    def _resolve_path(self, asset: AssetRef) -> Path:
        """Turn an AssetRef back into an on-disk file path."""
        if asset.namespace:
            candidate = Path(asset.namespace) / asset.name
            for ext in _SUPPORTED_EXTENSIONS:
                p = candidate.with_suffix(ext)
                if p.exists():
                    return p
        # search from root
        for f in self._files:
            if f.stem == asset.name:
                return f
        raise FileNotFoundError(f"Cannot resolve asset {asset.name}")

    def _asset_for(self, table: str) -> AssetRef:
        for f in self._files:
            if f.stem == table:
                return AssetRef(
                    source_id=str(self._root), asset_type="file",
                    name=f.stem, namespace=str(f.parent),
                )
        return AssetRef(source_id=str(self._root), asset_type="file", name=table)
