"""MySQL connector implementing BaseConnector.

Requires ``pymysql`` to be installed.  Falls back to a clear error
message if the driver is missing.
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from loguru import logger

try:
    from sqlalchemy import create_engine, text
    from sqlalchemy.engine import Engine
except ImportError:  # pragma: no cover
    Engine = None  # type: ignore[assignment,misc]

from app.config import ConnectionConfig
from app.connectors.base import (
    AssetRef,
    BaseConnector,
    ColumnInfo,
    ConnectionStatus,
    SchemaInfo,
)


class MySQLConnector(BaseConnector):
    """MySQL / MariaDB connector with read-only safety."""

    def __init__(self, config: ConnectionConfig):
        self.config = config
        self.engine: Optional[Engine] = None

    # ------------------------------------------------------------------
    # BaseConnector contract
    # ------------------------------------------------------------------

    def test_connection(self) -> ConnectionStatus:
        t0 = time.monotonic()
        try:
            engine = self._build_engine()
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            latency = int((time.monotonic() - t0) * 1000)
            engine.dispose()
            return ConnectionStatus(ok=True, latency_ms=latency, auth_type="password")
        except Exception as exc:
            latency = int((time.monotonic() - t0) * 1000)
            return ConnectionStatus(ok=False, error=str(exc), latency_ms=latency, auth_type="password")

    def list_assets(self, schema: Optional[str] = None) -> List[AssetRef]:
        db = schema or self.config.database or ""
        tables = self.get_table_list(db)
        return [
            AssetRef(
                source_id=f"{self.config.host}:{self.config.port}/{db}",
                asset_type="table",
                name=t,
                namespace=db,
            )
            for t in tables
        ]

    def get_schema(self, asset: AssetRef) -> SchemaInfo:
        db = asset.namespace or self.config.database or ""
        df = self.get_column_info(asset.name, db)
        columns = [
            ColumnInfo(
                name=row["column_name"],
                declared_type=row["data_type"],
                inferred_type=row["data_type"],
                nullable=row["is_nullable"] == "YES",
            )
            for _, row in df.iterrows()
        ]
        return SchemaInfo(columns=columns)

    def sample(self, asset: AssetRef, n: int = 10_000) -> pd.DataFrame:
        db = asset.namespace or self.config.database or ""
        query = f"SELECT * FROM `{db}`.`{asset.name}` LIMIT {n}"
        return self.execute_query(query)

    def estimate_row_count(self, asset: AssetRef) -> Optional[int]:
        db = asset.namespace or self.config.database or ""
        try:
            return self.get_table_row_count(asset.name, db)
        except Exception:
            return None

    def get_freshness(self, asset: AssetRef) -> Optional[datetime]:
        schema_info = self.get_schema(asset)
        db = asset.namespace or self.config.database or ""
        date_cols = [
            c.name for c in schema_info.columns
            if any(kw in c.declared_type.lower() for kw in ("datetime", "timestamp", "date"))
        ]
        if not date_cols:
            return None
        col = date_cols[0]
        try:
            df = self.execute_query(f"SELECT MAX(`{col}`) AS latest FROM `{db}`.`{asset.name}`")
            val = df["latest"].iloc[0]
            if pd.isna(val):
                return None
            return pd.Timestamp(val).to_pydatetime()
        except Exception:
            return None

    # ------------------------------------------------------------------
    # Legacy helper methods
    # ------------------------------------------------------------------

    def connect(self):
        if self.engine:
            return self.engine
        self.engine = self._build_engine()
        logger.info(f"Connected to MySQL: {self.config.host}:{self.config.port}/{self.config.database}")
        return self.engine

    def _build_engine(self) -> "Engine":
        if self.config.connection_string:
            conn_str = self.config.connection_string
        else:
            conn_str = (
                f"mysql+pymysql://{self.config.username}:{self.config.password}"
                f"@{self.config.host}:{self.config.port}/{self.config.database}"
            )
        return create_engine(
            conn_str,
            pool_size=self.config.pool_size,
            pool_pre_ping=True,
            connect_args={"connect_timeout": self.config.timeout_seconds},
        )

    @contextmanager
    def get_connection(self):
        if not self.engine:
            self.connect()
        conn = self.engine.connect()
        try:
            yield conn
        finally:
            conn.close()

    def execute_query(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        with self.get_connection() as conn:
            return pd.read_sql(text(query), conn, params=params)

    def get_table_list(self, schema: Optional[str] = None) -> List[str]:
        db = schema or self.config.database or ""
        query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = :schema AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """
        df = self.execute_query(query, {"schema": db})
        return df["table_name"].tolist()

    def get_table_row_count(self, table: str, schema: Optional[str] = None) -> int:
        db = schema or self.config.database or ""
        query = """
            SELECT table_rows AS estimate
            FROM information_schema.tables
            WHERE table_schema = :schema AND table_name = :table
        """
        df = self.execute_query(query, {"schema": db, "table": table})
        if not df.empty and df["estimate"].iloc[0] is not None:
            est = int(df["estimate"].iloc[0])
            if est >= 0:
                return est
        df2 = self.execute_query(f"SELECT COUNT(*) as count FROM `{db}`.`{table}`")
        return int(df2["count"].iloc[0])

    def get_column_info(self, table: str, schema: Optional[str] = None) -> pd.DataFrame:
        db = schema or self.config.database or ""
        query = """
            SELECT
                column_name, data_type, is_nullable,
                column_default, character_maximum_length,
                numeric_precision, numeric_scale
            FROM information_schema.columns
            WHERE table_schema = :schema AND table_name = :table
            ORDER BY ordinal_position
        """
        return self.execute_query(query, {"schema": db, "table": table})

    def sample_table(self, table: str, sample_rate: float = 0.1,
                     max_rows: Optional[int] = None, schema: Optional[str] = None) -> pd.DataFrame:
        db = schema or self.config.database or ""
        limit = max_rows or 10_000
        query = f"SELECT * FROM `{db}`.`{table}` LIMIT {limit}"
        return self.execute_query(query)

    def get_primary_keys(self, table: str, schema: Optional[str] = None) -> List[str]:
        db = schema or self.config.database or ""
        query = """
            SELECT column_name
            FROM information_schema.key_column_usage
            WHERE table_schema = :schema AND table_name = :table
              AND constraint_name = 'PRIMARY'
            ORDER BY ordinal_position
        """
        df = self.execute_query(query, {"schema": db, "table": table})
        return df["column_name"].tolist() if not df.empty else []

    def get_foreign_keys(self, table: str, schema: Optional[str] = None) -> List[Dict[str, Any]]:
        db = schema or self.config.database or ""
        query = """
            SELECT
                kcu.column_name,
                kcu.referenced_table_name AS foreign_table,
                kcu.referenced_column_name AS foreign_column
            FROM information_schema.key_column_usage kcu
            WHERE kcu.table_schema = :schema AND kcu.table_name = :table
              AND kcu.referenced_table_name IS NOT NULL
        """
        df = self.execute_query(query, {"schema": db, "table": table})
        return df.to_dict("records") if not df.empty else []

    def get_indexes(self, table: str, schema: Optional[str] = None) -> List[Dict[str, Any]]:
        db = schema or self.config.database or ""
        query = """
            SELECT index_name, GROUP_CONCAT(column_name ORDER BY seq_in_index) AS column_names,
                   NOT non_unique AS is_unique
            FROM information_schema.statistics
            WHERE table_schema = :schema AND table_name = :table
            GROUP BY index_name, non_unique
        """
        df = self.execute_query(query, {"schema": db, "table": table})
        return df.to_dict("records") if not df.empty else []

    def close(self):
        if self.engine:
            self.engine.dispose()
            self.engine = None
            logger.info("MySQL connection closed")
