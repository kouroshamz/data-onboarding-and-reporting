"""PostgreSQL connector with read-only enforcement.

Implements BaseConnector (INTERFACE_CONTRACTS_V1.md §2) while keeping
backward-compatible helper methods used by the legacy pipeline code.
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from loguru import logger
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from app.config import ConnectionConfig
from app.connectors.base import (
    AssetRef,
    BaseConnector,
    ColumnInfo,
    ConnectionStatus,
    SchemaInfo,
)


class PostgreSQLConnector(BaseConnector):
    """PostgreSQL database connector with safety features."""

    def __init__(self, config: ConnectionConfig):
        self.config = config
        self.engine: Optional[Engine] = None

    # ------------------------------------------------------------------
    # BaseConnector contract methods
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

    def list_assets(self, schema: str = "public") -> List[AssetRef]:
        tables = self.get_table_list(schema)
        return [
            AssetRef(
                source_id=f"{self.config.host}:{self.config.port}/{self.config.database}",
                asset_type="table",
                name=t,
                namespace=schema,
            )
            for t in tables
        ]

    def get_schema(self, asset: AssetRef) -> SchemaInfo:
        schema = asset.namespace or "public"
        df = self.get_column_info(asset.name, schema)
        columns = [
            ColumnInfo(
                name=row["column_name"],
                declared_type=row["data_type"],
                inferred_type=row["data_type"],
                nullable=row["is_nullable"] == "YES",
                notes=None,
            )
            for _, row in df.iterrows()
        ]
        return SchemaInfo(columns=columns)

    def sample(self, asset: AssetRef, n: int = 10_000) -> pd.DataFrame:
        schema = asset.namespace or "public"
        query = f'SELECT * FROM "{schema}"."{asset.name}" LIMIT {n}'
        return self.execute_query(query)

    def estimate_row_count(self, asset: AssetRef) -> Optional[int]:
        schema = asset.namespace or "public"
        try:
            return self.get_table_row_count(asset.name, schema)
        except Exception:
            return None

    def get_freshness(self, asset: AssetRef) -> Optional[datetime]:
        """Find the max value of any date/timestamp column in the asset."""
        schema_info = self.get_schema(asset)
        ns = asset.namespace or "public"
        date_cols = [
            c.name
            for c in schema_info.columns
            if any(kw in c.declared_type.lower() for kw in ("timestamp", "date", "time"))
        ]
        if not date_cols:
            return None
        col = date_cols[0]
        try:
            df = self.execute_query(
                f'SELECT MAX("{col}") AS latest FROM "{ns}"."{asset.name}"'
            )
            val = df["latest"].iloc[0]
            if pd.isna(val):
                return None
            if isinstance(val, str):
                return datetime.fromisoformat(val)
            return pd.Timestamp(val).to_pydatetime()
        except Exception:
            return None

    # ------------------------------------------------------------------
    # Legacy helper methods (used by SchemaExtractor, SamplingStrategy …)
    # ------------------------------------------------------------------

    def connect(self) -> Engine:
        if self.engine:
            return self.engine
        self.engine = self._build_engine()
        if self.config.read_only:
            self._verify_read_only()
        logger.info(
            f"Connected to PostgreSQL: {self.config.host}:{self.config.port}/{self.config.database}"
        )
        return self.engine

    def _build_engine(self) -> Engine:
        if self.config.connection_string:
            conn_str = self.config.connection_string
        else:
            conn_str = (
                f"postgresql://{self.config.username}:{self.config.password}"
                f"@{self.config.host}:{self.config.port}/{self.config.database}"
            )
        return create_engine(
            conn_str,
            pool_size=self.config.pool_size,
            pool_pre_ping=True,
            connect_args={"connect_timeout": self.config.timeout_seconds},
        )

    def _verify_read_only(self):
        with self.engine.connect() as conn:
            conn.execute(text("SHOW transaction_read_only"))
            result = conn.execute(
                text(
                    "SELECT has_table_privilege(current_user, "
                    "'pg_catalog.pg_class', 'INSERT')"
                )
            )
            can_insert = result.scalar()
            if can_insert:
                logger.warning("Connection has write privileges - proceed with caution")
            else:
                logger.info("Read-only access confirmed")

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

    def get_table_list(self, schema: str = "public") -> List[str]:
        query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = :schema AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """
        df = self.execute_query(query, {"schema": schema})
        return df["table_name"].tolist()

    def get_table_row_count(self, table: str, schema: str = "public") -> int:
        query = """
            SELECT reltuples::bigint as estimate
            FROM pg_class
            WHERE relname = :table
            AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = :schema)
        """
        df = self.execute_query(query, {"table": table, "schema": schema})
        if not df.empty:
            estimate = df["estimate"].iloc[0]
            if estimate is not None and int(estimate) >= 0:
                return int(estimate)
        query = f'SELECT COUNT(*) as count FROM "{schema}"."{table}"'
        df = self.execute_query(query)
        return int(df["count"].iloc[0])

    def get_column_info(self, table: str, schema: str = "public") -> pd.DataFrame:
        query = """
            SELECT
                column_name, data_type, is_nullable,
                column_default, character_maximum_length,
                numeric_precision, numeric_scale
            FROM information_schema.columns
            WHERE table_schema = :schema AND table_name = :table
            ORDER BY ordinal_position
        """
        return self.execute_query(query, {"schema": schema, "table": table})

    def sample_table(
        self,
        table: str,
        sample_rate: float = 0.1,
        max_rows: Optional[int] = None,
        schema: str = "public",
    ) -> pd.DataFrame:
        sample_pct = int(sample_rate * 100)
        query = f'SELECT * FROM "{schema}"."{table}" TABLESAMPLE SYSTEM ({sample_pct})'
        if max_rows:
            query += f" LIMIT {max_rows}"
        return self.execute_query(query)

    def get_primary_keys(self, table: str, schema: str = "public") -> List[str]:
        query = """
            SELECT a.attname as column_name
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = (
                SELECT oid FROM pg_class
                WHERE relname = :table
                AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = :schema)
            ) AND i.indisprimary
        """
        df = self.execute_query(query, {"table": table, "schema": schema})
        return df["column_name"].tolist() if not df.empty else []

    def get_foreign_keys(self, table: str, schema: str = "public") -> List[Dict[str, Any]]:
        query = """
            SELECT
                kcu.column_name,
                ccu.table_name AS foreign_table,
                ccu.column_name AS foreign_column
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
                AND tc.table_name = :table AND tc.table_schema = :schema
        """
        df = self.execute_query(query, {"table": table, "schema": schema})
        return df.to_dict("records") if not df.empty else []

    def get_indexes(self, table: str, schema: str = "public") -> List[Dict[str, Any]]:
        query = """
            SELECT
                i.relname as index_name,
                array_agg(a.attname) as column_names,
                ix.indisunique as is_unique
            FROM pg_class t
            JOIN pg_index ix ON t.oid = ix.indrelid
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
            WHERE t.relname = :table
                AND t.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = :schema)
            GROUP BY i.relname, ix.indisunique
        """
        df = self.execute_query(query, {"table": table, "schema": schema})
        return df.to_dict("records") if not df.empty else []

    def close(self):
        if self.engine:
            self.engine.dispose()
            self.engine = None
            logger.info("Database connection closed")
