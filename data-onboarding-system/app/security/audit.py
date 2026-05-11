"""Audit logging for the data-onboarding system.

Provides immutable, structured audit records for:
- Data access events (who accessed what data, when)
- Configuration changes
- Pipeline run summaries
- PII exposure incidents

Conforms to SECURITY_BASELINE_V1.md §3.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from loguru import logger


class AuditLogger:
    """Append-only audit logger that writes JSON-lines to a file."""

    def __init__(self, log_dir: str | Path | None = None, filename: str = "audit.jsonl"):
        self._dir = Path(log_dir) if log_dir else Path("logs")
        self._dir.mkdir(parents=True, exist_ok=True)
        self._path = self._dir / filename

    # ------------------------------------------------------------------
    # Core logging
    # ------------------------------------------------------------------

    def log(
        self,
        event_type: str,
        *,
        actor: str = "system",
        resource: str = "",
        details: Dict[str, Any] | None = None,
        severity: str = "info",
    ) -> Dict[str, Any]:
        """Write one audit record and return it."""
        record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event_type": event_type,
            "actor": actor,
            "resource": resource,
            "severity": severity,
            "details": details or {},
        }
        self._append(record)
        logger.debug("Audit: {} – {} on {}", event_type, actor, resource)
        return record

    # Convenience wrappers

    def log_data_access(self, table: str, rows: int, actor: str = "system") -> Dict[str, Any]:
        return self.log("data_access", actor=actor, resource=table, details={"rows": rows})

    def log_pipeline_run(self, run_id: str, status: str, steps: int) -> Dict[str, Any]:
        return self.log(
            "pipeline_run",
            resource=run_id,
            details={"status": status, "steps": steps},
            severity="info" if status == "completed" else "warning",
        )

    def log_pii_detected(self, table: str, columns: List[str]) -> Dict[str, Any]:
        return self.log(
            "pii_detected",
            resource=table,
            details={"columns": columns, "count": len(columns)},
            severity="warning",
        )

    def log_config_change(self, field: str, old_value: Any, new_value: Any) -> Dict[str, Any]:
        return self.log(
            "config_change",
            details={"field": field, "old": str(old_value), "new": str(new_value)},
        )

    def log_error(self, message: str, **extra: Any) -> Dict[str, Any]:
        return self.log("error", details={"message": message, **extra}, severity="error")

    # ------------------------------------------------------------------
    # Read-back
    # ------------------------------------------------------------------

    def read_all(self) -> List[Dict[str, Any]]:
        """Read all audit records (for review / export)."""
        if not self._path.exists():
            return []
        records: List[Dict[str, Any]] = []
        with open(self._path) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
        return records

    def tail(self, n: int = 20) -> List[Dict[str, Any]]:
        """Return last *n* records."""
        return self.read_all()[-n:]

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _append(self, record: Dict[str, Any]) -> None:
        with open(self._path, "a") as f:
            f.write(json.dumps(record, default=str) + "\n")
