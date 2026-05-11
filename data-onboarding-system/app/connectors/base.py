"""Base connector contract and shared data types.

Implements the interface defined in handover/INTERFACE_CONTRACTS_V1.md §2.
Every connector must subclass BaseConnector and implement all abstract methods.
"""

from __future__ import annotations

import abc
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, List, Optional

import pandas as pd


# ---------------------------------------------------------------------------
# Contract data types
# ---------------------------------------------------------------------------

@dataclass
class AssetRef:
    """Reference to a single data asset (table, file, object)."""
    source_id: str
    asset_type: str  # "table", "file", "object"
    name: str
    namespace: Optional[str] = None

    @property
    def identifier(self) -> str:
        if self.namespace:
            return f"{self.namespace}.{self.name}"
        return self.name


@dataclass
class ColumnInfo:
    """Column metadata."""
    name: str
    declared_type: str
    inferred_type: str = ""
    nullable: bool = True
    notes: Optional[str] = None


@dataclass
class SchemaInfo:
    """Schema metadata for a single asset."""
    columns: List[ColumnInfo] = field(default_factory=list)


@dataclass
class ConnectionStatus:
    """Result of a connection test."""
    ok: bool
    error: Optional[str] = None
    latency_ms: Optional[int] = None
    auth_type: str = "password"


# ---------------------------------------------------------------------------
# Abstract base connector
# ---------------------------------------------------------------------------

class BaseConnector(abc.ABC):
    """Abstract base connector that every data-source adapter must implement.

    The six required methods match INTERFACE_CONTRACTS_V1.md §2.
    """

    @abc.abstractmethod
    def test_connection(self) -> ConnectionStatus:
        """Verify connectivity and credentials without heavy operations."""
        ...

    @abc.abstractmethod
    def list_assets(self) -> List[AssetRef]:
        """Enumerate all available assets (tables / files / objects)."""
        ...

    @abc.abstractmethod
    def get_schema(self, asset: AssetRef) -> SchemaInfo:
        """Return column-level metadata for a single asset."""
        ...

    @abc.abstractmethod
    def sample(self, asset: AssetRef, n: int = 10_000) -> pd.DataFrame:
        """Return up to *n* rows from the asset."""
        ...

    @abc.abstractmethod
    def estimate_row_count(self, asset: AssetRef) -> Optional[int]:
        """Fast (approximate) row count; return None if not available."""
        ...

    @abc.abstractmethod
    def get_freshness(self, asset: AssetRef) -> Optional[datetime]:
        """Return the most-recent timestamp value found in the asset."""
        ...

    # ------------------------------------------------------------------
    # Convenience helpers (non-abstract)
    # ------------------------------------------------------------------
    def close(self) -> None:  # noqa: B027  – intentionally empty
        """Release resources. Subclasses may override."""

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
