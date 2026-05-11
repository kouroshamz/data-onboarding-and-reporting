"""Response cache — hash prompt content to avoid duplicate LLM calls."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Optional

from loguru import logger


class ResponseCache:
    """File-backed SHA-256 content-hash cache for LLM responses."""

    def __init__(self, cache_dir: str | Path = ".llm_cache"):
        self._dir = Path(cache_dir)
        self._dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _hash(key: str) -> str:
        return hashlib.sha256(key.encode()).hexdigest()

    def get(self, messages_json: str) -> Optional[str]:
        """Return cached response content or None."""
        h = self._hash(messages_json)
        path = self._dir / f"{h}.json"
        if path.exists():
            try:
                data = json.loads(path.read_text())
                logger.debug("LLM cache hit: {}", h[:12])
                return data.get("content")
            except Exception:
                pass
        return None

    def put(self, messages_json: str, content: str, meta: dict | None = None) -> None:
        """Store response in cache."""
        h = self._hash(messages_json)
        path = self._dir / f"{h}.json"
        payload = {"content": content, "hash": h}
        if meta:
            payload["meta"] = meta
        path.write_text(json.dumps(payload, indent=2))
        logger.debug("LLM cache stored: {}", h[:12])

    def clear(self) -> int:
        """Remove all cached entries. Returns count deleted."""
        count = 0
        for f in self._dir.glob("*.json"):
            f.unlink()
            count += 1
        return count
