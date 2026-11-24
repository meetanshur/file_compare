"""Benchmark streaming fixture ingestion into the DuckDB storage layer."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import platform
import sys
import time
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import duckdb
from lxml import etree

from storage.duckdb_store import DuckDBStore, RecordSide

DEFAULT_BATCH_SIZE = 50_000
DEFAULT_MAX_MEMORY_MB = 512


def _sha256(path: Path, chunk_size: int = 4 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def _iter_csv_records(path: Path) -> Iterator[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames is None or not {"id", "value", "padding"}.issubset(reader.fieldnames):
            raise ValueError(f"{path.name} does not have the expected generated CSV columns")
        for row in reader:
            if None in row or any(value is None for value in row.values()):
                raise ValueError(f"{path.name} contains a malformed CSV row")
            yield row


def _iter_xml_records(path: Path) -> Iterator[dict[str, str]]:
    with path.open("rb") as stream:
        for _, element in etree.iterparse(stream, events=("end",), tag="record"):
            value = element.findtext("value")
            padding = element.findtext("padding")
            record_id = element.get("id")
            if record_id is None or value is None or padding is None:
                raise ValueError(f"{path.name} contains a malformed XML record")
            record = {"id": record_id, "value": value, "padding": padding}
            element.clear(keep_tail=True)
            parent = element.getparent()
            if parent is not None:
                while element.getprevious() is not None:
                    del parent[0]
            yield record


def _measure_parser(records: Iterator[dict[str, str]]) -> tuple[int, float]:
    started = time.perf_counter()
    count = sum(1 for _ in records)
    return count, time.perf_counter() - started


def _timed_records(
    records: Iterator[dict[str, str]], timings: dict[str, float]
) -> Iterator[dict[str, str]]:
    iterator = iter(records)
    while True:
        started = time.perf_counter()
        try:
            record = next(iterator)
        except StopIteration:
            timings["parser_seconds"] = timings.get("parser_seconds", 0.0) + (
                time.perf_counter() - started
            )
            return
        timings["parser_seconds"] = timings.get("parser_seconds", 0.0) + (
            time.perf_counter() - started
        )
        yield record


def _peak_memory_bytes() -> int:
    if os.name == "nt":
        import ctypes
        from ctypes import wintypes

        class ProcessMemoryCounters(ctypes.Structure):
            _fields_ = [
                ("cb", wintypes.DWORD),
                ("PageFaultCount", wintypes.DWORD),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
            ]

        kernel32 = ctypes.WinDLL("Kernel32", use_last_error=True)
        kernel32.GetCurrentProcess.restype = wintypes.HANDLE
        psapi = ctypes.WinDLL("Psapi", use_last_error=True)
        get_process_memory_info = psapi.GetProcessMemoryInfo
        get_process_memory_info.argtypes = [
            wintypes.HANDLE,
            ctypes.POINTER(ProcessMemoryCounters),
            wintypes.DWORD,
        ]
        get_process_memory_info.restype = wintypes.BOOL
        counters = ProcessMemoryCounters()
        counters.cb = ctypes.sizeof(counters)
        if not get_process_memory_info(
            kernel32.GetCurrentProcess(), ctypes.byref(counters), counters.cb
        ):
            raise OSError(ctypes.get_last_error(), "Could not read process peak working set")
        return int(counters.PeakWorkingSetSize)

    import resource

    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(peak if sys.platform == "darwin" else peak * 1024)


def run_benchmark(
    manifest_path: str | Path,
    *,
    database_directory: str | Path | None = None,
    max_memory_mb: int = DEFAULT_MAX_MEMORY_MB,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> dict[str, Any]:
    """Hash and stream both fixture files into DuckDB, returning measurements."""
    manifest_file = Path(manifest_path).resolve()
    if manifest_file.parent.name != ".test_data":
        raise ValueError("The manifest must be stored in an ignored .test_data directory")
    if max_memory_mb < 2:
        raise ValueError("max_memory_mb must be at least 2")
    if batch_size < 1:
        raise ValueError("batch_size must be greater than zero")

    with manifest_file.open(encoding="utf-8") as stream:
        manifest = json.load(stream)
    file_format = manifest.get("format")
    if file_format not in {"xml", "csv"}:
        raise ValueError("Manifest format must be 'xml' or 'csv'")

    fixture_paths: list[tuple[str, RecordSide, Path]] = []
    for label, side in (("file_a", RecordSide.FILE_A), ("file_b", RecordSide.FILE_B)):
        file_info = manifest["files"][label]
        fixture_path = (manifest_file.parent / file_info["name"]).resolve()
        if fixture_path.parent != manifest_file.parent:
            raise ValueError("Fixture paths in manifest must be adjacent to the manifest")
        fixture_paths.append((label, side, fixture_path))

    db_directory = (
        Path(database_directory).resolve()
        if database_directory is not None
        else manifest_file.parent / "benchmark_storage"
    )
    if (
        db_directory.name != "benchmark_storage"
        and manifest_file.parent not in db_directory.parents
    ):
        raise ValueError(
            "Benchmark databases must be stored under the fixture .test_data directory"
        )
    db_directory.mkdir(parents=True, exist_ok=True)
    free_disk_bytes_before = _free_disk_bytes(db_directory)
    run_id = uuid4()
    database_path = db_directory / f"benchmark_{run_id}.duckdb"
    duckdb_memory_mb = max(1, max_memory_mb // 2)
    expected_count = manifest["record_counts"]
    expected_size = int(manifest["size_bytes_per_file"])
    parse_records = _iter_csv_records if file_format == "csv" else _iter_xml_records
    measurements: list[dict[str, Any]] = []
    started_at = datetime.now(UTC)
    total_started = time.perf_counter()

    with DuckDBStore(
        database_path,
        job_id=run_id,
        max_memory_mb=duckdb_memory_mb,
        batch_size=batch_size,
        max_total_differences=0,
    ) as store:
        for label, side, fixture_path in fixture_paths:
            actual_size = fixture_path.stat().st_size
            if actual_size != expected_size:
                raise ValueError(
                    f"{fixture_path.name} is {actual_size} bytes; manifest expects {expected_size}"
                )

            hash_started = time.perf_counter()
            actual_digest = _sha256(fixture_path)
            hash_seconds = time.perf_counter() - hash_started
            expected_digest = manifest["files"][label]["sha256"]
            if actual_digest != expected_digest:
                raise ValueError(f"SHA-256 mismatch for {fixture_path.name}")

            parsed_count, parser_only_seconds = _measure_parser(parse_records(fixture_path))
            if parsed_count != int(expected_count[label]):
                raise ValueError(
                    f"{fixture_path.name} parser produced {parsed_count} records; "
                    f"manifest expects {expected_count[label]}"
                )

            ingest_started = time.perf_counter()
            parser_timings = {"parser_seconds": 0.0}
            if file_format == "csv":
                record_count = store.insert_csv_file(side, fixture_path, key_fields=("id",))
                parser_seconds = None
                storage_seconds = None
                ingest_method = "DuckDB native read_csv plus SQL insert"
            else:
                record_count = store.insert_records(
                    side,
                    _timed_records(parse_records(fixture_path), parser_timings),
                    key_fields=("id",),
                )
                parser_seconds = parser_timings["parser_seconds"]
                storage_seconds = max(0.0, time.perf_counter() - ingest_started - parser_seconds)
                ingest_method = "lxml iterparse plus DuckDB Appender"
            ingest_seconds = time.perf_counter() - ingest_started
            if record_count != int(expected_count[label]):
                raise ValueError(
                    f"{fixture_path.name} produced {record_count} records; "
                    f"manifest expects {expected_count[label]}"
                )
            measurements.append(
                {
                    "file": fixture_path.name,
                    "side": side.value,
                    "size_bytes": actual_size,
                    "sha256_valid": True,
                    "records_ingested": record_count,
                    "expected_records": int(expected_count[label]),
                    "hash_seconds": hash_seconds,
                    "parser_only_seconds": parser_only_seconds,
                    "ingest_seconds": ingest_seconds,
                    "ingest_method": ingest_method,
                    "parser_seconds_during_ingest": parser_seconds,
                    "storage_seconds_estimate": storage_seconds,
                    "ingest_mib_per_second": actual_size / (1024**2) / ingest_seconds
                    if ingest_seconds
                    else None,
                    "rows_per_second": record_count / ingest_seconds if ingest_seconds else None,
                }
            )

        stored_counts = {
            "file_a": store.count_records(RecordSide.FILE_A),
            "file_b": store.count_records(RecordSide.FILE_B),
        }
        if stored_counts != {key: int(value) for key, value in expected_count.items()}:
            raise ValueError(f"Persisted record counts do not match manifest: {stored_counts}")

    total_seconds = time.perf_counter() - total_started
    total_bytes = sum(item["size_bytes"] for item in measurements)
    total_rows = sum(item["records_ingested"] for item in measurements)
    peak_memory_bytes = _peak_memory_bytes()
    return {
        "schema_version": 1,
        "benchmark_scope": "streaming parse and DuckDB record ingestion; no record diff joins",
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(UTC).isoformat(),
        "format": file_format,
        "manifest": manifest_file.name,
        "runtime": {
            "python": platform.python_version(),
            "duckdb": duckdb.__version__,
            "platform": platform.platform(),
            "processor": platform.processor(),
        },
        "settings": {
            "max_memory_mb": max_memory_mb,
            "duckdb_memory_mb": duckdb_memory_mb,
            "batch_size": batch_size,
        },
        "database": str(database_path),
        "free_disk_bytes_before": free_disk_bytes_before,
        "files": measurements,
        "validation": {
            "hashes_match_manifest": all(item["sha256_valid"] for item in measurements),
            "record_counts_match_manifest": True,
            "peak_memory_within_budget": peak_memory_bytes <= max_memory_mb * 1024**2,
            "expected_difference_manifest_counts": {
                kind: len(entries) for kind, entries in manifest["expected_differences"].items()
            },
            "difference_comparison_performed": False,
        },
        "totals": {
            "bytes_ingested": total_bytes,
            "records_ingested": total_rows,
            "elapsed_seconds": total_seconds,
            "mib_per_second": total_bytes / (1024**2) / total_seconds if total_seconds else None,
            "rows_per_second": total_rows / total_seconds if total_seconds else None,
            "peak_process_memory_bytes": peak_memory_bytes,
            "hash_seconds": sum(item["hash_seconds"] for item in measurements),
            "parser_only_seconds": sum(item["parser_only_seconds"] for item in measurements),
            "csv_native_parse_and_insert_seconds": sum(
                item["ingest_seconds"]
                for item in measurements
                if item["ingest_method"] == "DuckDB native read_csv plus SQL insert"
            ),
            "xml_parser_seconds_during_ingest": sum(
                item["parser_seconds_during_ingest"] or 0 for item in measurements
            ),
            "xml_storage_seconds_estimate": sum(
                item["storage_seconds_estimate"] or 0 for item in measurements
            ),
        },
    }


def _free_disk_bytes(path: Path) -> int:
    import shutil

    return shutil.disk_usage(path).free


def main(argv: list[str] | None = None) -> int:
    """Run the large-file ingestion benchmark CLI."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest", required=True, type=Path, help="fixture expected-differences JSON"
    )
    parser.add_argument("--database-directory", type=Path)
    parser.add_argument("--report", type=Path)
    parser.add_argument("--max-memory-mb", type=int, default=DEFAULT_MAX_MEMORY_MB)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    args = parser.parse_args(argv)

    try:
        report = run_benchmark(
            args.manifest,
            database_directory=args.database_directory,
            max_memory_mb=args.max_memory_mb,
            batch_size=args.batch_size,
        )
        report_path = (
            args.report
            if args.report is not None
            else args.manifest.parent / f"{args.manifest.stem}_benchmark.json"
        )
        if report_path.resolve().parent.name != ".test_data":
            parser.error("Benchmark report must be written directly into a .test_data directory")
        with report_path.open("w", encoding="utf-8", newline="\n") as stream:
            json.dump(report, stream, indent=2)
            stream.write("\n")
    except (OSError, KeyError, ValueError, json.JSONDecodeError) as exc:
        print(f"Benchmark failed: {exc}", file=sys.stderr)
        return 1

    print(f"Benchmark report: {report_path}")
    print(json.dumps(report["totals"], indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
