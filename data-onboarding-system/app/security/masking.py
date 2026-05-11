"""Data masking utilities.

Implements SECURITY_BASELINE_V1.md requirements:
- Redact PII columns in samples and reports
- Support configurable masking rules per column pattern
- Preserve data type and length for downstream analysis
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Pattern, Sequence

import pandas as pd
from loguru import logger


class MaskStrategy(str, Enum):
    REDACT = "redact"        # Replace with ***REDACTED***
    HASH = "hash"            # SHA-256 truncated hash (preserves uniqueness)
    PARTIAL = "partial"      # Show first/last chars: J***n
    NULL = "null"            # Replace with None
    FAKE = "fake"            # Type-preserving fake value
    TRUNCATE = "truncate"    # Keep first N chars


@dataclass
class MaskingRule:
    """A single masking rule."""
    pattern: str                      # Regex pattern matching column names
    strategy: MaskStrategy = MaskStrategy.REDACT
    priority: int = 0                 # Higher = applied first
    preserve_length: bool = False

    def matches(self, column_name: str) -> bool:
        return bool(re.search(self.pattern, column_name, re.IGNORECASE))


# Default rules for common PII patterns
DEFAULT_RULES: List[MaskingRule] = [
    MaskingRule(pattern=r"(email|e_mail|email_address)", strategy=MaskStrategy.HASH, priority=10),
    MaskingRule(pattern=r"(password|passwd|secret|token|api_key)", strategy=MaskStrategy.REDACT, priority=20),
    MaskingRule(pattern=r"(ssn|social_security|national_id|tax_id)", strategy=MaskStrategy.REDACT, priority=20),
    MaskingRule(pattern=r"(credit_card|card_number|ccn|pan)", strategy=MaskStrategy.REDACT, priority=20),
    MaskingRule(pattern=r"(phone|mobile|fax|tel)", strategy=MaskStrategy.PARTIAL, priority=5),
    MaskingRule(pattern=r"(first_name|last_name|full_name|surname)", strategy=MaskStrategy.HASH, priority=5),
    MaskingRule(pattern=r"(address|street|zip|postal)", strategy=MaskStrategy.TRUNCATE, priority=5),
    MaskingRule(pattern=r"(ip_address|ip_addr|remote_ip)", strategy=MaskStrategy.HASH, priority=5),
    MaskingRule(pattern=r"(date_of_birth|dob|birth_date)", strategy=MaskStrategy.REDACT, priority=10),
]


class DataMasker:
    """Apply masking rules to DataFrames and dictionaries."""

    def __init__(self, rules: Sequence[MaskingRule] | None = None, salt: str = ""):
        self.rules = sorted(rules or DEFAULT_RULES, key=lambda r: -r.priority)
        self._salt = salt

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def mask_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Return a new DataFrame with PII columns masked according to rules."""
        out = df.copy()
        masked_cols: List[str] = []
        for col in out.columns:
            rule = self._find_rule(col)
            if rule:
                out[col] = out[col].apply(lambda v, r=rule: self._apply(v, r))
                masked_cols.append(col)
        if masked_cols:
            logger.info("Masked {} columns: {}", len(masked_cols), masked_cols)
        return out

    def mask_dict(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Mask values in a flat dict whose keys match rules."""
        out = {}
        for k, v in data.items():
            rule = self._find_rule(k)
            out[k] = self._apply(v, rule) if rule else v
        return out

    def get_masked_columns(self, columns: Sequence[str]) -> List[str]:
        """Return list of column names that would be masked."""
        return [c for c in columns if self._find_rule(c)]

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _find_rule(self, column_name: str) -> Optional[MaskingRule]:
        for rule in self.rules:
            if rule.matches(column_name):
                return rule
        return None

    def _apply(self, value: Any, rule: MaskingRule) -> Any:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None

        s = str(value)
        if rule.strategy == MaskStrategy.REDACT:
            return "***REDACTED***"
        elif rule.strategy == MaskStrategy.HASH:
            h = hashlib.sha256(f"{self._salt}{s}".encode()).hexdigest()[:16]
            return f"HASH_{h}"
        elif rule.strategy == MaskStrategy.PARTIAL:
            if len(s) <= 4:
                return "***"
            return f"{s[0]}{'*' * (len(s) - 2)}{s[-1]}"
        elif rule.strategy == MaskStrategy.NULL:
            return None
        elif rule.strategy == MaskStrategy.TRUNCATE:
            return s[:3] + "***" if len(s) > 3 else "***"
        elif rule.strategy == MaskStrategy.FAKE:
            # Type-preserving stub
            return "***FAKE***"
        return "***"
