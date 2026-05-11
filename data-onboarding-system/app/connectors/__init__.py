"""Connectors package – unified data-source adapters.

Use :func:`create_connector` as a single-entry-point factory.
"""

from __future__ import annotations

from typing import Any

from app.connectors.base import (
    AssetRef,
    BaseConnector,
    ColumnInfo,
    ConnectionStatus,
    SchemaInfo,
)
from app.connectors.postgres import PostgreSQLConnector
from app.connectors.csv_connector import CSVConnector

__all__ = [
    "AssetRef",
    "BaseConnector",
    "ColumnInfo",
    "ConnectionStatus",
    "SchemaInfo",
    "PostgreSQLConnector",
    "CSVConnector",
    "create_connector",
]


def create_connector(config: Any) -> BaseConnector:
    """Factory: return the right connector for *config.type*.

    Parameters
    ----------
    config : ConnectionConfig (or any object with a ``.type`` attribute)

    Returns
    -------
    BaseConnector subclass instance
    """
    ctype = getattr(config, "type", "").lower()

    if ctype == "postgresql":
        return PostgreSQLConnector(config)

    if ctype == "mysql":
        from app.connectors.mysql_connector import MySQLConnector
        return MySQLConnector(config)

    if ctype in ("csv", "file"):
        return CSVConnector(config)

    if ctype == "s3":
        from app.connectors.s3_connector import S3Connector
        return S3Connector(config)

    raise NotImplementedError(
        f"Connector type '{ctype}' is not supported. "
        "Available types: postgresql, mysql, csv, s3"
    )
