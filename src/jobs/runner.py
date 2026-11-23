"""Background execution and progress tracking for comparison jobs."""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, TypeVar
from uuid import UUID, uuid4

from common.comparison_config import ComparisonConfig
from storage.duckdb_store import DuckDBStore

logger = logging.getLogger(__name__)

JobResult = TypeVar("JobResult")
JobWorker = Callable[["ComparisonJobContext"], JobResult]


class JobStatus(StrEnum):
    """Lifecycle state of a comparison job."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    TRUNCATED = "truncated"
    FAILED = "failed"
    CANCELLED = "cancelled"


class JobCapacityError(RuntimeError):
    """The runner has reached its configured active-job capacity."""


class JobNotFoundError(KeyError):
    """No job with the requested identifier exists in this runner."""


class JobCancelledError(Exception):
    """Raised by a worker when its job has been cancelled."""


@dataclass(frozen=True)
class JobSnapshot:
    """Immutable status and progress information for a job."""

    job_id: UUID
    status: JobStatus
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    percent: float | None
    rows_processed: int
    total_rows: int | None
    eta_seconds: float | None
    result: Any = None
    error: str | None = None


@dataclass
class _JobState:
    job_id: UUID
    config: ComparisonConfig
    worker: JobWorker[Any]
    status: JobStatus = JobStatus.PENDING
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    started_at: datetime | None = None
    started_monotonic: float | None = None
    finished_at: datetime | None = None
    rows_processed: int = 0
    total_rows: int | None = None
    result: Any = None
    error: str | None = None
    cancel_event: threading.Event = field(default_factory=threading.Event)
    future: Future[None] | None = None


class ComparisonJobContext:
    """Resources and cooperative controls supplied to a comparison worker."""

    def __init__(
        self,
        job_id: UUID,
        config: ComparisonConfig,
        store: DuckDBStore,
        cancel_event: threading.Event,
        report_progress: Callable[[int, int | None], None],
    ) -> None:
        self.job_id = job_id
        self.config = config
        self.store = store
        self._cancel_event = cancel_event
        self._report_progress = report_progress

    def report_progress(self, rows_processed: int, total_rows: int | None = None) -> None:
        """Update row counts; ETA is estimated only when a total is known."""
        self.raise_if_cancelled()
        self._report_progress(rows_processed, total_rows)

    def raise_if_cancelled(self) -> None:
        """Allow long-running workers to stop promptly when cancellation is requested."""
        if self._cancel_event.is_set():
            raise JobCancelledError("Comparison job was cancelled")


class ComparisonJobRunner:
    """Run comparisons in background threads with a DuckDB store per job."""

    def __init__(
        self,
        database_directory: str | Path,
        *,
        max_workers: int = 2,
        max_active_jobs: int = 8,
    ) -> None:
        if max_workers < 1:
            raise ValueError("max_workers must be greater than zero")
        if max_active_jobs < max_workers:
            raise ValueError("max_active_jobs must be at least max_workers")

        self.database_directory = Path(database_directory)
        self.database_directory.mkdir(parents=True, exist_ok=True)
        self.max_active_jobs = max_active_jobs
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="file-compare",
        )
        self._lock = threading.RLock()
        self._jobs: dict[UUID, _JobState] = {}
        self._closed = False

    def submit(
        self,
        worker: JobWorker[JobResult],
        *,
        config: ComparisonConfig | None = None,
    ) -> UUID:
        """Submit a worker and immediately return its job identifier."""
        if not callable(worker):
            raise TypeError("worker must be callable")

        job_config = (config if config is not None else ComparisonConfig()).model_copy(deep=True)
        with self._lock:
            if self._closed:
                raise RuntimeError("Comparison job runner is shut down")
            active_count = sum(
                state.status in {JobStatus.PENDING, JobStatus.RUNNING}
                for state in self._jobs.values()
            )
            if active_count >= self.max_active_jobs:
                raise JobCapacityError("The maximum number of active comparison jobs was reached")

            job_id = uuid4()
            state = _JobState(job_id=job_id, config=job_config, worker=worker)
            self._jobs[job_id] = state
            state.future = self._executor.submit(self._execute, state)
            return job_id

    def get_job(self, job_id: UUID) -> JobSnapshot:
        """Return a consistent snapshot of a known job."""
        with self._lock:
            state = self._jobs.get(job_id)
            if state is None:
                raise JobNotFoundError(str(job_id))
            return self._snapshot(state, time.monotonic())

    def cancel(self, job_id: UUID) -> JobSnapshot:
        """Request cancellation; running workers must check their context."""
        with self._lock:
            state = self._jobs.get(job_id)
            if state is None:
                raise JobNotFoundError(str(job_id))
            if state.status in {
                JobStatus.COMPLETED,
                JobStatus.TRUNCATED,
                JobStatus.FAILED,
                JobStatus.CANCELLED,
            }:
                return self._snapshot(state, time.monotonic())

            state.cancel_event.set()
            if state.future is not None and state.future.cancel():
                state.status = JobStatus.CANCELLED
                state.finished_at = datetime.now(UTC)
            return self._snapshot(state, time.monotonic())

    def wait(self, job_id: UUID, timeout: float | None = None) -> JobSnapshot:
        """Wait for a job to finish and return its final snapshot."""
        with self._lock:
            state = self._jobs.get(job_id)
            if state is None:
                raise JobNotFoundError(str(job_id))
            future = state.future
        if future is not None:
            future.result(timeout=timeout)
        return self.get_job(job_id)

    def _execute(self, state: _JobState) -> None:
        with self._lock:
            if state.status is JobStatus.CANCELLED:
                return
            state.status = JobStatus.RUNNING
            state.started_at = datetime.now(UTC)
            state.started_monotonic = time.monotonic()

        database_path = self.database_directory / f"{state.job_id}.duckdb"
        duckdb_memory_mb = max(1, state.config.max_memory_mb // 2)

        try:
            with DuckDBStore(
                database_path,
                job_id=state.job_id,
                max_memory_mb=duckdb_memory_mb,
                max_total_differences=state.config.max_total_differences,
            ) as store:
                context = ComparisonJobContext(
                    state.job_id,
                    state.config,
                    store,
                    state.cancel_event,
                    lambda rows, total: self._update_progress(state, rows, total),
                )
                result = state.worker(context)
                context.raise_if_cancelled()
                final_status = JobStatus.TRUNCATED if store.truncated else JobStatus.COMPLETED
                with self._lock:
                    state.result = result
                    state.status = final_status
                    state.finished_at = datetime.now(UTC)
        except JobCancelledError:
            with self._lock:
                state.status = JobStatus.CANCELLED
                state.finished_at = datetime.now(UTC)
        except Exception as exc:
            logger.exception("Comparison job %s failed", state.job_id)
            with self._lock:
                state.status = JobStatus.FAILED
                state.error = str(exc) or type(exc).__name__
                state.finished_at = datetime.now(UTC)

    def _update_progress(
        self,
        state: _JobState,
        rows_processed: int,
        total_rows: int | None,
    ) -> None:
        if rows_processed < 0:
            raise ValueError("rows_processed must be zero or greater")
        if total_rows is not None and total_rows < 0:
            raise ValueError("total_rows must be zero or greater")
        if total_rows is not None and rows_processed > total_rows:
            raise ValueError("rows_processed cannot exceed total_rows")

        with self._lock:
            if rows_processed < state.rows_processed:
                raise ValueError("rows_processed cannot move backwards")
            state.rows_processed = rows_processed
            if total_rows is not None:
                state.total_rows = total_rows
            elif state.total_rows is not None and rows_processed > state.total_rows:
                raise ValueError("rows_processed cannot exceed the previously reported total")

    def _snapshot(self, state: _JobState, now_monotonic: float) -> JobSnapshot:
        percent: float | None = None
        eta_seconds: float | None = None
        if state.status in {JobStatus.COMPLETED, JobStatus.TRUNCATED}:
            percent = 100.0
            eta_seconds = 0.0
        elif state.total_rows is not None:
            if state.total_rows == 0:
                percent = 0.0
            else:
                percent = min(100.0, state.rows_processed * 100.0 / state.total_rows)
                if state.rows_processed > 0 and state.started_monotonic is not None:
                    elapsed = now_monotonic - state.started_monotonic
                    if elapsed > 0 and state.total_rows > state.rows_processed:
                        eta_seconds = (
                            elapsed
                            * (state.total_rows - state.rows_processed)
                            / state.rows_processed
                        )
        return JobSnapshot(
            job_id=state.job_id,
            status=state.status,
            created_at=state.created_at,
            started_at=state.started_at,
            finished_at=state.finished_at,
            percent=percent,
            rows_processed=state.rows_processed,
            total_rows=state.total_rows,
            eta_seconds=eta_seconds,
            result=state.result,
            error=state.error,
        )

    def shutdown(self, *, wait: bool = True, cancel_pending: bool = False) -> None:
        """Stop accepting jobs and shut down worker threads."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            if cancel_pending:
                for state in self._jobs.values():
                    if state.status in {JobStatus.PENDING, JobStatus.RUNNING}:
                        self.cancel(state.job_id)
        self._executor.shutdown(wait=wait, cancel_futures=cancel_pending)

    def __enter__(self) -> ComparisonJobRunner:
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.shutdown()
