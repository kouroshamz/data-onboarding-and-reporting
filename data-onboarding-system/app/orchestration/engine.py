"""Pipeline execution engine.

Implements the orchestration contract from PROJECT_SPEC_V1.md §7:
- Run ID for idempotency
- Status tracking (queued → running → completed | failed | blocked)
- Bounded retries per step
- Progress callbacks
"""

from __future__ import annotations

import enum
import time
import traceback as tb
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Sequence

from loguru import logger


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

class PipelineStatus(str, enum.Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    BLOCKED = "blocked"


@dataclass
class StepResult:
    """Outcome of a single pipeline step."""
    name: str
    status: PipelineStatus
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    duration_s: float = 0.0
    error: Optional[str] = None
    retries_used: int = 0
    output: Any = None


@dataclass
class PipelineRun:
    """Full record of one pipeline execution."""
    run_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    status: PipelineStatus = PipelineStatus.QUEUED
    steps: List[StepResult] = field(default_factory=list)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    finished_at: Optional[datetime] = None
    error: Optional[str] = None

    @property
    def duration_s(self) -> float:
        if self.finished_at:
            return (self.finished_at - self.created_at).total_seconds()
        return (datetime.now(timezone.utc) - self.created_at).total_seconds()

    def summary(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "status": self.status.value,
            "steps_total": len(self.steps),
            "steps_completed": sum(1 for s in self.steps if s.status == PipelineStatus.COMPLETED),
            "steps_failed": sum(1 for s in self.steps if s.status == PipelineStatus.FAILED),
            "duration_s": round(self.duration_s, 2),
        }


# ---------------------------------------------------------------------------
# Step definition
# ---------------------------------------------------------------------------

@dataclass
class StepDef:
    """Definition of a pipeline step."""
    name: str
    fn: Callable[..., Any]
    max_retries: int = 2
    depends_on: List[str] = field(default_factory=list)
    timeout_s: float = 0  # 0 = no timeout
    description: str = ""


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------

class PipelineEngine:
    """Sequential pipeline runner with retry, dependency checks, and status tracking."""

    def __init__(
        self,
        steps: Sequence[StepDef] | None = None,
        max_retries: int = 2,
        on_step_start: Callable[[str, int], None] | None = None,
        on_step_end: Callable[[StepResult], None] | None = None,
    ):
        self._steps: List[StepDef] = list(steps) if steps else []
        self._default_retries = max_retries
        self._on_start = on_step_start
        self._on_end = on_step_end
        self._runs: Dict[str, PipelineRun] = {}

    # ------------------------------------------------------------------
    # Step management
    # ------------------------------------------------------------------

    def add_step(
        self,
        name: str,
        fn: Callable[..., Any],
        *,
        max_retries: int | None = None,
        depends_on: List[str] | None = None,
        timeout_s: float = 0,
        description: str = "",
    ) -> "PipelineEngine":
        self._steps.append(
            StepDef(
                name=name,
                fn=fn,
                max_retries=max_retries if max_retries is not None else self._default_retries,
                depends_on=depends_on or [],
                timeout_s=timeout_s,
                description=description,
            )
        )
        return self

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def run(self, context: Dict[str, Any] | None = None) -> PipelineRun:
        """Execute all steps sequentially. Returns the PipelineRun record.

        *context* is passed as first positional arg to every step function.
        """
        ctx = context or {}
        pipeline = PipelineRun()
        self._runs[pipeline.run_id] = pipeline
        pipeline.status = PipelineStatus.RUNNING
        logger.info("Pipeline {} started ({} steps)", pipeline.run_id, len(self._steps))

        completed_steps: set[str] = set()

        for step_def in self._steps:
            # Dependency check
            unmet = set(step_def.depends_on) - completed_steps
            if unmet:
                sr = StepResult(
                    name=step_def.name,
                    status=PipelineStatus.BLOCKED,
                    error=f"Blocked by unmet dependencies: {sorted(unmet)}",
                )
                pipeline.steps.append(sr)
                logger.warning("Step '{}' blocked: {}", step_def.name, sr.error)
                continue

            sr = self._execute_step(step_def, ctx)
            pipeline.steps.append(sr)

            if sr.status == PipelineStatus.COMPLETED:
                completed_steps.add(step_def.name)
                # Merge step output into context
                if isinstance(sr.output, dict):
                    ctx.update(sr.output)
            elif sr.status == PipelineStatus.FAILED:
                pipeline.status = PipelineStatus.FAILED
                pipeline.error = f"Step '{step_def.name}' failed: {sr.error}"
                pipeline.finished_at = datetime.now(timezone.utc)
                logger.error("Pipeline {} failed at step '{}'", pipeline.run_id, step_def.name)
                return pipeline

        pipeline.status = PipelineStatus.COMPLETED
        pipeline.finished_at = datetime.now(timezone.utc)
        logger.info("Pipeline {} completed in {:.1f}s", pipeline.run_id, pipeline.duration_s)
        return pipeline

    def _execute_step(self, step_def: StepDef, ctx: Dict[str, Any]) -> StepResult:
        """Execute a single step with bounded retries."""
        sr = StepResult(name=step_def.name, status=PipelineStatus.RUNNING)
        sr.started_at = datetime.now(timezone.utc)

        if self._on_start:
            self._on_start(step_def.name, 0)

        last_error: str | None = None
        for attempt in range(step_def.max_retries + 1):
            try:
                logger.debug("Step '{}' attempt {}/{}", step_def.name, attempt + 1, step_def.max_retries + 1)
                result = step_def.fn(ctx)
                sr.status = PipelineStatus.COMPLETED
                sr.output = result
                sr.retries_used = attempt
                break
            except Exception as exc:  # noqa: BLE001
                last_error = f"{type(exc).__name__}: {exc}"
                logger.warning("Step '{}' attempt {} failed: {}", step_def.name, attempt + 1, last_error)
                sr.retries_used = attempt + 1
                if attempt < step_def.max_retries:
                    time.sleep(min(2 ** attempt, 10))  # Exponential backoff, max 10s
        else:
            sr.status = PipelineStatus.FAILED
            sr.error = last_error

        sr.finished_at = datetime.now(timezone.utc)
        sr.duration_s = (sr.finished_at - sr.started_at).total_seconds()

        if self._on_end:
            self._on_end(sr)

        return sr

    # ------------------------------------------------------------------
    # History
    # ------------------------------------------------------------------

    def get_run(self, run_id: str) -> Optional[PipelineRun]:
        return self._runs.get(run_id)

    @property
    def last_run(self) -> Optional[PipelineRun]:
        if not self._runs:
            return None
        return list(self._runs.values())[-1]

    def history(self) -> List[Dict[str, Any]]:
        return [r.summary() for r in self._runs.values()]
