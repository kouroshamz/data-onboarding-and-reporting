"""Tests for orchestration engine."""

import pytest
from app.orchestration.engine import PipelineEngine, PipelineRun, PipelineStatus, StepDef


def test_pipeline_completes_all_steps():
    engine = PipelineEngine()
    engine.add_step("step1", lambda ctx: {"a": 1})
    engine.add_step("step2", lambda ctx: {"b": ctx.get("a", 0) + 1})

    result = engine.run()

    assert isinstance(result, PipelineRun)
    assert result.status == PipelineStatus.COMPLETED
    assert len(result.steps) == 2
    assert all(s.status == PipelineStatus.COMPLETED for s in result.steps)


def test_pipeline_fails_on_error():
    engine = PipelineEngine(max_retries=0)
    engine.add_step("good", lambda ctx: {"ok": True})
    engine.add_step("bad", lambda ctx: (_ for _ in ()).throw(ValueError("boom")))
    engine.add_step("skipped", lambda ctx: None)

    result = engine.run()

    assert result.status == PipelineStatus.FAILED
    assert result.steps[0].status == PipelineStatus.COMPLETED
    assert result.steps[1].status == PipelineStatus.FAILED
    assert "boom" in result.steps[1].error
    assert len(result.steps) == 2  # skipped step never runs


def test_pipeline_retries_on_failure():
    call_count = {"n": 0}

    def flaky(ctx):
        call_count["n"] += 1
        if call_count["n"] < 3:
            raise RuntimeError("transient")
        return {"done": True}

    engine = PipelineEngine(max_retries=3)
    engine.add_step("flaky_step", flaky)

    result = engine.run()

    assert result.status == PipelineStatus.COMPLETED
    assert result.steps[0].retries_used == 2  # 2 retries before success


def test_pipeline_blocks_on_unmet_dependency():
    engine = PipelineEngine()
    engine.add_step("step2", lambda ctx: None, depends_on=["step1_missing"])

    result = engine.run()

    assert result.steps[0].status == PipelineStatus.BLOCKED


def test_pipeline_run_id_is_unique():
    engine = PipelineEngine()
    engine.add_step("s", lambda ctx: None)
    r1 = engine.run()
    r2 = engine.run()
    assert r1.run_id != r2.run_id


def test_pipeline_summary():
    engine = PipelineEngine()
    engine.add_step("a", lambda ctx: None)
    engine.add_step("b", lambda ctx: None)
    result = engine.run()
    summary = result.summary()

    assert summary["steps_total"] == 2
    assert summary["steps_completed"] == 2
    assert summary["steps_failed"] == 0
    assert "run_id" in summary
    assert "duration_s" in summary


def test_pipeline_context_flows_between_steps():
    """Step outputs should be merged into context for subsequent steps."""
    engine = PipelineEngine()
    engine.add_step("produce", lambda ctx: {"value": 42})
    engine.add_step("consume", lambda ctx: {"doubled": ctx["value"] * 2})

    result = engine.run()
    assert result.steps[1].output == {"doubled": 84}
