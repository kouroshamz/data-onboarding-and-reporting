"""Local file-system artifact storage.

Stores run artifacts (reports, profiles, samples) under a configurable
root directory with run-ID based subdirectories for isolation.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional

from loguru import logger


class LocalStorage:
    """Manage pipeline artifacts on the local filesystem."""

    def __init__(self, root: str | Path = "reports", retention_days: int = 90):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.retention_days = retention_days

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def save_json(self, run_id: str, name: str, data: Any) -> Path:
        """Save JSON data to ``<root>/<run_id>/<name>.json``."""
        p = self._ensure_dir(run_id) / f"{name}.json"
        with open(p, "w") as f:
            json.dump(data, f, indent=2, default=str)
        logger.debug("Saved JSON artifact: {}", p)
        return p

    def save_text(self, run_id: str, name: str, content: str, ext: str = "txt") -> Path:
        """Save plain text artifact."""
        p = self._ensure_dir(run_id) / f"{name}.{ext}"
        p.write_text(content)
        logger.debug("Saved text artifact: {}", p)
        return p

    def save_bytes(self, run_id: str, name: str, data: bytes, ext: str = "bin") -> Path:
        """Save binary artifact (e.g. PDF)."""
        p = self._ensure_dir(run_id) / f"{name}.{ext}"
        p.write_bytes(data)
        logger.debug("Saved binary artifact: {}", p)
        return p

    def copy_file(self, run_id: str, src: str | Path) -> Path:
        """Copy an existing file into the run directory."""
        src_path = Path(src)
        dest = self._ensure_dir(run_id) / src_path.name
        shutil.copy2(src_path, dest)
        return dest

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def load_json(self, run_id: str, name: str) -> Any:
        """Load a JSON artifact."""
        p = self.root / run_id / f"{name}.json"
        if not p.exists():
            raise FileNotFoundError(f"Artifact not found: {p}")
        with open(p) as f:
            return json.load(f)

    def list_runs(self) -> List[str]:
        """List all run IDs with artifacts."""
        return sorted(
            d.name for d in self.root.iterdir() if d.is_dir() and not d.name.startswith(".")
        )

    def list_artifacts(self, run_id: str) -> List[str]:
        """List artifact files for a run."""
        d = self.root / run_id
        if not d.exists():
            return []
        return sorted(f.name for f in d.iterdir() if f.is_file())

    def get_path(self, run_id: str, filename: str) -> Path:
        """Return the full path for an artifact; does not check existence."""
        return self.root / run_id / filename

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def cleanup_old_runs(self) -> int:
        """Remove run directories older than *retention_days*. Returns count removed."""
        import time

        cutoff = time.time() - self.retention_days * 86400
        removed = 0
        for d in self.root.iterdir():
            if d.is_dir() and d.stat().st_mtime < cutoff:
                shutil.rmtree(d)
                removed += 1
                logger.info("Cleaned up old run directory: {}", d.name)
        return removed

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _ensure_dir(self, run_id: str) -> Path:
        d = self.root / run_id
        d.mkdir(parents=True, exist_ok=True)
        return d
