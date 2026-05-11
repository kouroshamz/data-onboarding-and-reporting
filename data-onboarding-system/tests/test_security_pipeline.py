"""Integration tests for Audit logging, Data masking, and Pipeline engine."""

import json
import time
from pathlib import Path

import pandas as pd
import pytest

from app.security.audit import AuditLogger
from app.security.masking import DataMasker, MaskStrategy, MaskingRule
from app.orchestration.engine import PipelineEngine, PipelineRun, PipelineStatus, StepDef


# =========================================================================
# AuditLogger
# =========================================================================

class TestAuditLogger:

    @pytest.fixture
    def audit(self, tmp_path):
        return AuditLogger(log_dir=tmp_path, filename="test_audit.jsonl")

    def test_log_creates_record(self, audit):
        rec = audit.log("test_event", actor="tester", resource="table_x")
        assert rec["event_type"] == "test_event"
        assert rec["actor"] == "tester"
        assert rec["resource"] == "table_x"
        assert "timestamp" in rec

    def test_records_persist_to_file(self, audit, tmp_path):
        audit.log("event_1")
        audit.log("event_2")
        content = (tmp_path / "test_audit.jsonl").read_text()
        lines = [l for l in content.strip().split("\n") if l]
        assert len(lines) == 2

    def test_read_all_returns_records(self, audit):
        audit.log("a")
        audit.log("b")
        audit.log("c")
        records = audit.read_all()
        assert len(records) == 3

    def test_tail_returns_last_n(self, audit):
        for i in range(10):
            audit.log(f"event_{i}")
        last_3 = audit.tail(3)
        assert len(last_3) == 3
        assert last_3[-1]["event_type"] == "event_9"

    def test_log_data_access(self, audit):
        rec = audit.log_data_access("users", 500, actor="pipeline")
        assert rec["event_type"] == "data_access"
        assert rec["details"]["rows"] == 500

    def test_log_pipeline_run(self, audit):
        rec = audit.log_pipeline_run("run-abc", "completed", 6)
        assert rec["event_type"] == "pipeline_run"
        assert rec["details"]["status"] == "completed"
        assert rec["severity"] == "info"

    def test_log_pipeline_run_failed(self, audit):
        rec = audit.log_pipeline_run("run-fail", "failed", 3)
        assert rec["severity"] == "warning"

    def test_log_pii_detected(self, audit):
        rec = audit.log_pii_detected("customers", ["email", "ssn"])
        assert rec["event_type"] == "pii_detected"
        assert rec["details"]["count"] == 2
        assert rec["severity"] == "warning"

    def test_log_config_change(self, audit):
        rec = audit.log_config_change("timeout", 30, 60)
        assert rec["event_type"] == "config_change"
        assert rec["details"]["field"] == "timeout"

    def test_log_error(self, audit):
        rec = audit.log_error("Something broke", step="stage3")
        assert rec["event_type"] == "error"
        assert rec["severity"] == "error"
        assert rec["details"]["step"] == "stage3"

    def test_empty_read(self, tmp_path):
        audit2 = AuditLogger(log_dir=tmp_path, filename="empty.jsonl")
        assert audit2.read_all() == []


# =========================================================================
# Data Masker
# =========================================================================

class TestDataMasker:

    def test_default_rules_mask_email(self):
        masker = DataMasker()
        df = pd.DataFrame({"email": ["alice@example.com", "bob@test.com"]})
        masked = masker.mask_dataframe(df)
        assert all(v.startswith("HASH_") for v in masked["email"])

    def test_default_rules_mask_password(self):
        masker = DataMasker()
        df = pd.DataFrame({"password": ["secret123", "hunter2"]})
        masked = masker.mask_dataframe(df)
        assert all(v == "***REDACTED***" for v in masked["password"])

    def test_default_rules_mask_phone(self):
        masker = DataMasker()
        df = pd.DataFrame({"phone": ["555-1234", "555-5678"]})
        masked = masker.mask_dataframe(df)
        # Partial masking: first and last char visible
        for v in masked["phone"]:
            assert "*" in v

    def test_custom_rules(self):
        rules = [MaskingRule(pattern=r"salary", strategy=MaskStrategy.REDACT)]
        masker = DataMasker(rules=rules)
        df = pd.DataFrame({"salary": [50000, 60000], "name": ["Alice", "Bob"]})
        masked = masker.mask_dataframe(df)
        assert all(v == "***REDACTED***" for v in masked["salary"])
        # Name should NOT be masked (our custom rules don't match it)
        assert list(masked["name"]) == ["Alice", "Bob"]

    def test_mask_dict(self):
        masker = DataMasker()
        data = {"email": "alice@test.com", "age": 30, "city": "Berlin"}
        masked = masker.mask_dict(data)
        assert masked["email"].startswith("HASH_")
        assert masked["age"] == 30
        assert masked["city"] == "Berlin"

    def test_get_masked_columns(self):
        masker = DataMasker()
        cols = ["id", "email", "name", "phone", "ssn", "city"]
        masked = masker.get_masked_columns(cols)
        assert "email" in masked
        assert "phone" in masked
        assert "ssn" in masked
        assert "id" not in masked
        assert "city" not in masked

    def test_null_handling(self):
        masker = DataMasker()
        df = pd.DataFrame({"email": [None, "test@test.com", float("nan")]})
        masked = masker.mask_dataframe(df)
        assert masked["email"].iloc[0] is None
        assert masked["email"].iloc[2] is None
        assert masked["email"].iloc[1].startswith("HASH_")

    def test_hash_strategy_preserves_uniqueness(self):
        masker = DataMasker()
        df = pd.DataFrame({"email": ["a@test.com", "b@test.com", "a@test.com"]})
        masked = masker.mask_dataframe(df)
        # Same input → same hash
        assert masked["email"].iloc[0] == masked["email"].iloc[2]
        # Different input → different hash
        assert masked["email"].iloc[0] != masked["email"].iloc[1]

    def test_truncate_strategy(self):
        rules = [MaskingRule(pattern=r"address", strategy=MaskStrategy.TRUNCATE)]
        masker = DataMasker(rules=rules)
        df = pd.DataFrame({"address": ["123 Main Street"]})
        masked = masker.mask_dataframe(df)
        assert masked["address"].iloc[0] == "123***"

    def test_null_strategy(self):
        rules = [MaskingRule(pattern=r"secret", strategy=MaskStrategy.NULL)]
        masker = DataMasker(rules=rules)
        df = pd.DataFrame({"secret": ["hidden_value"]})
        masked = masker.mask_dataframe(df)
        assert masked["secret"].iloc[0] is None


# =========================================================================
# Pipeline Engine
# =========================================================================

class TestPipelineEngine:

    def test_empty_pipeline(self):
        engine = PipelineEngine()
        run = engine.run()
        assert run.status == PipelineStatus.COMPLETED
        assert len(run.steps) == 0

    def test_single_step_success(self):
        engine = PipelineEngine()
        engine.add_step("greet", lambda ctx: {"message": "hello"})
        run = engine.run()
        assert run.status == PipelineStatus.COMPLETED
        assert len(run.steps) == 1
        assert run.steps[0].status == PipelineStatus.COMPLETED

    def test_multi_step_pipeline(self):
        engine = PipelineEngine()
        results = []
        engine.add_step("step1", lambda ctx: results.append("s1") or {"val": 1})
        engine.add_step("step2", lambda ctx: results.append("s2") or {"val": 2})
        engine.add_step("step3", lambda ctx: results.append("s3") or {"val": 3})
        run = engine.run()
        assert run.status == PipelineStatus.COMPLETED
        assert len(run.steps) == 3
        assert results == ["s1", "s2", "s3"]

    def test_step_failure_stops_pipeline(self):
        engine = PipelineEngine(max_retries=0)
        engine.add_step("good", lambda ctx: {})
        engine.add_step("bad", lambda ctx: (_ for _ in ()).throw(ValueError("boom")))
        engine.add_step("never", lambda ctx: {})
        run = engine.run()
        assert run.status == PipelineStatus.FAILED
        assert run.steps[0].status == PipelineStatus.COMPLETED
        assert run.steps[1].status == PipelineStatus.FAILED
        assert len(run.steps) == 2  # step 3 never ran

    def test_step_retry_succeeds(self):
        attempts = {"count": 0}

        def flaky(ctx):
            attempts["count"] += 1
            if attempts["count"] < 2:
                raise RuntimeError("Transient error")
            return {"ok": True}

        engine = PipelineEngine(max_retries=2)
        engine.add_step("flaky", flaky)
        run = engine.run()
        assert run.status == PipelineStatus.COMPLETED
        assert run.steps[0].retries_used == 1

    def test_step_retry_exhausted(self):
        def always_fail(ctx):
            raise RuntimeError("Permanent error")

        engine = PipelineEngine(max_retries=1)
        engine.add_step("always_fail", always_fail)
        run = engine.run()
        assert run.status == PipelineStatus.FAILED
        assert run.steps[0].retries_used == 2  # initial + 1 retry

    def test_dependency_check(self):
        engine = PipelineEngine(max_retries=0)
        engine.add_step("setup", lambda ctx: {"ready": True})
        engine.add_step("process", lambda ctx: {}, depends_on=["setup"])
        run = engine.run()
        assert run.status == PipelineStatus.COMPLETED
        assert all(s.status == PipelineStatus.COMPLETED for s in run.steps)

    def test_dependency_blocked(self):
        engine = PipelineEngine(max_retries=0)
        engine.add_step("fail", lambda ctx: (_ for _ in ()).throw(ValueError("fail")))
        engine.add_step("blocked", lambda ctx: {}, depends_on=["fail"])
        run = engine.run()
        # Pipeline fails at "fail" step and "blocked" never reached
        assert run.status == PipelineStatus.FAILED

    def test_context_propagation(self):
        """Step output should be merged into context for later steps."""
        engine = PipelineEngine()
        engine.add_step("producer", lambda ctx: {"data": [1, 2, 3]})
        engine.add_step("consumer", lambda ctx: {"received": ctx.get("data")})
        run = engine.run()
        assert run.status == PipelineStatus.COMPLETED
        assert run.steps[1].output == {"received": [1, 2, 3]}

    def test_step_callbacks(self):
        starts = []
        ends = []
        engine = PipelineEngine(
            on_step_start=lambda name, attempt: starts.append(name),
            on_step_end=lambda result: ends.append(result.name),
        )
        engine.add_step("a", lambda ctx: {})
        engine.add_step("b", lambda ctx: {})
        engine.run()
        assert starts == ["a", "b"]
        assert ends == ["a", "b"]

    def test_run_id_unique(self):
        engine = PipelineEngine()
        engine.add_step("x", lambda ctx: {})
        r1 = engine.run()
        r2 = engine.run()
        assert r1.run_id != r2.run_id

    def test_history(self):
        engine = PipelineEngine()
        engine.add_step("x", lambda ctx: {})
        engine.run()
        engine.run()
        assert len(engine.history()) == 2

    def test_last_run(self):
        engine = PipelineEngine()
        engine.add_step("x", lambda ctx: {})
        assert engine.last_run is None
        engine.run()
        assert engine.last_run is not None
        assert engine.last_run.status == PipelineStatus.COMPLETED

    def test_summary_format(self):
        engine = PipelineEngine()
        engine.add_step("a", lambda ctx: {})
        run = engine.run()
        summary = run.summary()
        assert "run_id" in summary
        assert "status" in summary
        assert summary["steps_total"] == 1
        assert summary["steps_completed"] == 1
