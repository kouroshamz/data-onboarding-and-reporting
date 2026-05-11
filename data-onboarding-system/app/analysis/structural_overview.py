"""Dataset structural overview — Section 2 of the onboarding report.

Computes high-level dataset metrics:
  - Duplicate rows
  - Fully null columns
  - Constant columns
  - Data-type distribution
  - Estimated memory usage
  - Suspicious ID columns
"""

from __future__ import annotations

import os
from typing import Any, Dict, List

import numpy as np
import pandas as pd
from loguru import logger


def compute_structural_overview(
    schema_data: Dict[str, Any],
    profile_results: Dict[str, Any],
    sample_frames: Dict[str, pd.DataFrame],
    source_file_paths: List[str] | None = None,
) -> Dict[str, Any]:
    """Return a structural overview dict for the whole dataset."""

    total_rows = 0
    total_cols = 0
    total_duplicate_rows = 0
    total_memory_bytes = 0
    columns_with_nulls = 0
    columns_fully_null = 0
    constant_columns: List[str] = []
    suspicious_id_columns: List[str] = []
    dtype_distribution: Dict[str, int] = {}

    per_table: Dict[str, Any] = {}

    for table_name, profile in profile_results.items():
        df = sample_frames.get(table_name)
        cols = profile.get("columns", {})
        n_rows = profile.get("sample_size", 0)
        n_cols = len(cols)
        total_rows += n_rows
        total_cols += n_cols

        # Duplicate rows
        dupes = 0
        if df is not None and not df.empty:
            dupes = int(df.duplicated().sum())
            total_duplicate_rows += dupes
            total_memory_bytes += int(df.memory_usage(deep=True).sum())

        # Column-level flags
        tbl_nulls = 0
        tbl_fully_null = 0
        tbl_constant = []
        tbl_suspicious_id = []

        for col_name, cp in cols.items():
            null_pct = cp.get("null_percent", 0)
            unique_count = cp.get("unique_count", 0)
            unique_pct = cp.get("unique_percent", 0)
            type_cat = cp.get("type_category", cp.get("dtype", "unknown"))

            # Dtype distribution
            dtype_distribution[type_cat] = dtype_distribution.get(type_cat, 0) + 1

            # Nulls
            if null_pct > 0:
                tbl_nulls += 1
                columns_with_nulls += 1
            if null_pct >= 99.9:
                tbl_fully_null += 1
                columns_fully_null += 1

            # Constant columns (1 unique value)
            if unique_count <= 1 and null_pct < 100:
                tbl_constant.append(col_name)
                constant_columns.append(f"{table_name}.{col_name}")

            # Suspicious ID columns
            if _is_suspicious_id(col_name, unique_pct, type_cat, n_rows):
                tbl_suspicious_id.append(col_name)
                suspicious_id_columns.append(f"{table_name}.{col_name}")

        per_table[table_name] = {
            "rows": n_rows,
            "columns": n_cols,
            "duplicate_rows": dupes,
            "duplicate_pct": round(dupes / max(n_rows, 1) * 100, 2),
            "columns_with_nulls": tbl_nulls,
            "columns_fully_null": tbl_fully_null,
            "constant_columns": tbl_constant,
            "suspicious_id_columns": tbl_suspicious_id,
            "memory_bytes": int(df.memory_usage(deep=True).sum()) if df is not None and not df.empty else 0,
        }

    # File size
    file_size_bytes = 0
    if source_file_paths:
        for p in source_file_paths:
            try:
                file_size_bytes += os.path.getsize(p)
            except OSError:
                pass

    return {
        "total_rows": total_rows,
        "total_columns": total_cols,
        "total_duplicate_rows": total_duplicate_rows,
        "duplicate_pct": round(total_duplicate_rows / max(total_rows, 1) * 100, 2),
        "columns_with_nulls": columns_with_nulls,
        "columns_fully_null": columns_fully_null,
        "constant_columns": constant_columns,
        "suspicious_id_columns": suspicious_id_columns,
        "dtype_distribution": dtype_distribution,
        "estimated_memory_bytes": total_memory_bytes,
        "estimated_memory_mb": round(total_memory_bytes / (1024 * 1024), 2),
        "file_size_bytes": file_size_bytes,
        "file_size_mb": round(file_size_bytes / (1024 * 1024), 2) if file_size_bytes else None,
        "tables": per_table,
    }


def _is_suspicious_id(
    col_name: str,
    unique_pct: float,
    type_cat: str,
    n_rows: int,
) -> bool:
    """Heuristic: is this column likely an ID / primary key?"""
    name_lower = col_name.lower()
    id_keywords = {"id", "uid", "uuid", "key", "pk", "index", "idx", "code", "ref"}

    # Name contains ID-like token
    name_match = any(kw in name_lower.replace("_", " ").split() for kw in id_keywords)
    # Or ends with _id
    name_match = name_match or name_lower.endswith("_id") or name_lower == "id"

    # Very high uniqueness
    high_unique = unique_pct > 95 and n_rows > 10

    return name_match and high_unique
