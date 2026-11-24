import threading

import duckdb
import pytest

from common.comparison_config import ComparisonConfig
from common.comparison_result import ComparisonType, Difference
from jobs.runner import (
    ComparisonJobRunner,
    JobCapacityError,
    JobStatus,
)
from storage.duckdb_store import StorageError


def test_job_uses_half_of_configured_memory_for_duckdb(tmp_path) -> None:
    config = ComparisonConfig(max_memory_mb=64, duckdb_threads=2)

    def worker(context):
        memory_limit, temp_directory, threads, preserve_insertion_order = (
            context.store._require_connection()
            .execute(
                "SELECT current_setting('memory_limit'), current_setting('temp_directory'), "
                "current_setting('threads'), current_setting('preserve_insertion_order')"
            )
            .fetchone()
        )
        context.report_progress(10, 10)
        return memory_limit, temp_directory, threads, preserve_insertion_order

    with ComparisonJobRunner(tmp_path / "jobs", max_workers=1) as runner:
        job_id = runner.submit(worker, config=config)
        snapshot = runner.wait(job_id, timeout=10)

    assert snapshot.status is JobStatus.COMPLETED
    assert snapshot.result[0] == "32.0 MiB"
    assert snapshot.result[1].endswith(f"{job_id}_tmp")
    assert snapshot.result[2:] == (2, False)
    assert not (tmp_path / "jobs" / f"{job_id}_tmp").exists()
    assert snapshot.percent == 100
    assert snapshot.rows_processed == 10
    assert snapshot.eta_seconds == 0


def test_job_reports_progress_and_estimated_time(tmp_path) -> None:
    started = threading.Event()
    release = threading.Event()

    def worker(context):
        context.report_progress(25, 100)
        started.set()
        assert release.wait(timeout=10)

    with ComparisonJobRunner(tmp_path / "jobs", max_workers=1) as runner:
        job_id = runner.submit(worker)
        assert started.wait(timeout=10)
        snapshot = runner.get_job(job_id)
        release.set()
        completed = runner.wait(job_id, timeout=10)

    assert snapshot.status is JobStatus.RUNNING
    assert snapshot.percent == 25
    assert snapshot.rows_processed == 25
    assert snapshot.total_rows == 100
    assert snapshot.eta_seconds is not None
    assert snapshot.eta_seconds > 0
    assert completed.status is JobStatus.COMPLETED


def test_job_failure_is_visible_in_status(tmp_path) -> None:
    def worker(_context):
        raise RuntimeError("comparison failed")

    with ComparisonJobRunner(tmp_path / "jobs", max_workers=1) as runner:
        job_id = runner.submit(worker)
        snapshot = runner.wait(job_id, timeout=10)

    assert snapshot.status is JobStatus.FAILED
    assert snapshot.error == "comparison failed"
    assert not (tmp_path / "jobs" / f"{job_id}_tmp").exists()


def test_job_error_includes_and_logs_original_duckdb_error(tmp_path, caplog) -> None:
    def worker(_context):
        error = duckdb.OutOfMemoryException("could not allocate block of size 256.0 KiB")
        raise StorageError("Could not compare stored records in DuckDB.") from error

    with ComparisonJobRunner(tmp_path / "jobs", max_workers=1) as runner:
        job_id = runner.submit(worker)
        snapshot = runner.wait(job_id, timeout=10)

    assert snapshot.status is JobStatus.FAILED
    assert "Could not compare stored records in DuckDB." in snapshot.error
    assert "could not allocate block of size 256.0 KiB" in snapshot.error
    assert "could not allocate block of size 256.0 KiB" in caplog.text


def test_job_status_is_truncated_when_finding_limit_is_reached(tmp_path) -> None:
    def worker(context):
        context.store.insert_differences(
            Difference(
                comparison_type=ComparisonType.FIELD,
                location=str(index),
                message="Values differ",
            )
            for index in range(2)
        )

    with ComparisonJobRunner(tmp_path / "jobs", max_workers=1) as runner:
        job_id = runner.submit(worker, config=ComparisonConfig(max_total_differences=1))
        snapshot = runner.wait(job_id, timeout=10)

    assert snapshot.status is JobStatus.TRUNCATED


def test_running_job_can_be_cancelled_cooperatively(tmp_path) -> None:
    started = threading.Event()
    release = threading.Event()

    def worker(context):
        started.set()
        assert release.wait(timeout=10)
        context.raise_if_cancelled()

    with ComparisonJobRunner(tmp_path / "jobs", max_workers=1) as runner:
        job_id = runner.submit(worker)
        assert started.wait(timeout=10)
        runner.cancel(job_id)
        release.set()
        snapshot = runner.wait(job_id, timeout=10)

    assert snapshot.status is JobStatus.CANCELLED
    assert not (tmp_path / "jobs" / f"{job_id}_tmp").exists()


def test_runner_rejects_jobs_above_capacity(tmp_path) -> None:
    started = threading.Event()
    release = threading.Event()

    def worker(_context):
        started.set()
        assert release.wait(timeout=10)

    with ComparisonJobRunner(
        tmp_path / "jobs",
        max_workers=1,
        max_active_jobs=1,
    ) as runner:
        job_id = runner.submit(worker)
        assert started.wait(timeout=10)
        with pytest.raises(JobCapacityError):
            runner.submit(worker)
        release.set()
        assert runner.wait(job_id, timeout=10).status is JobStatus.COMPLETED
