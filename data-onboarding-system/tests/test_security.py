"""Tests for security masking and audit logging."""

import json
import pandas as pd
import pytest
from pathlib import Path

from app.security.masking import DataMasker, MaskingRule, MaskStrategy
from app.security.audit import AuditLogger


# ---------------------------------------------------------------------------
# DataMasker
# ---------------------------------------------------------------------------


def test_masker_redacts_email_column():
    df = pd.DataFrame({"email": ["alice@ex.com", "bob@ex.com"], "age": [30, 25]})
    masker = DataMasker()
    masked = masker.mask_dataframe(df)

    assert all("HASH_" in str(v) for v in masked["email"])
    assert list(masked["age"]) == [30, 25]  # Non-PII untouched


def test_masker_redacts_password():
    df = pd.DataFrame({"password": ["s3cret"], "id": [1]})
    masked = DataMasker().mask_dataframe(df)
    assert masked["password"].iloc[0] == "***REDACTED***"


def test_masker_partial_phone():
    df = pd.DataFrame({"phone": ["555-1234"]})
    masked = DataMasker().mask_dataframe(df)
    val = masked["phone"].iloc[0]
    assert val.startswith("5")
    assert val.endswith("4")
    assert "*" in val


def test_masker_preserves_nulls():
    df = pd.DataFrame({"email": [None, "x@y.com"]})
    masked = DataMasker().mask_dataframe(df)
    assert pd.isna(masked["email"].iloc[0])


def test_masker_get_masked_columns():
    masker = DataMasker()
    cols = masker.get_masked_columns(["id", "email", "phone", "name"])
    assert "email" in cols
    assert "phone" in cols
    assert "id" not in cols


def test_masker_custom_rules():
    rules = [MaskingRule(pattern=r"^secret_", strategy=MaskStrategy.NULL)]
    masker = DataMasker(rules=rules)
    df = pd.DataFrame({"secret_key": ["abc"], "public_key": ["xyz"]})
    masked = masker.mask_dataframe(df)
    assert masked["secret_key"].iloc[0] is None
    assert masked["public_key"].iloc[0] == "xyz"


def test_masker_mask_dict():
    masker = DataMasker()
    data = {"email": "test@x.com", "id": 123}
    result = masker.mask_dict(data)
    assert "HASH_" in result["email"]
    assert result["id"] == 123


# ---------------------------------------------------------------------------
# AuditLogger
# ---------------------------------------------------------------------------


def test_audit_log_writes_jsonl(tmp_path):
    audit = AuditLogger(log_dir=tmp_path)
    audit.log("test_event", actor="user1", resource="orders")

    records = audit.read_all()
    assert len(records) == 1
    assert records[0]["event_type"] == "test_event"
    assert records[0]["actor"] == "user1"


def test_audit_log_data_access(tmp_path):
    audit = AuditLogger(log_dir=tmp_path)
    audit.log_data_access("users", 100, actor="pipeline")
    records = audit.read_all()
    assert records[0]["details"]["rows"] == 100


def test_audit_log_pii_detected(tmp_path):
    audit = AuditLogger(log_dir=tmp_path)
    audit.log_pii_detected("customers", ["email", "ssn"])
    records = audit.read_all()
    assert records[0]["severity"] == "warning"
    assert records[0]["details"]["count"] == 2


def test_audit_tail(tmp_path):
    audit = AuditLogger(log_dir=tmp_path)
    for i in range(5):
        audit.log(f"event_{i}")
    last_two = audit.tail(2)
    assert len(last_two) == 2
    assert last_two[-1]["event_type"] == "event_4"
