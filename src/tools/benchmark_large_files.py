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
import xml.etree.ElementTree as ET
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import duckdb

from storage.duckdb_store import DuckDBStore, RecordSide

DEFAULT_BATCH_SIZE = 1_000
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
        stack: list[ET.Element] = []
        for event, element in ET.iterparse(stream, events=("start", "end")):
            if event == "start":
                stack.append(element)
                continue

            if element.tag == "record":
                value = element.findtext("value")
                padding = element.findtext("padding")
                record_id = element.get("id")
                if record_id is None or value is None or padding is None:
                    raise ValueError(f"{path.name} contains a malformed XML record")
                record = {"id": record_id, "value": value, "padding": padding}
                if len(stack) > 1:
                    stack[-2].remove(element)
                element.clear()
                yield record

            stack.pop()


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

            ingest_started = time.perf_counter()
            record_count = store.insert_records(
                side,
                parse_records(fixture_path),
                key_fields=("id",),
            )
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
                    "ingest_seconds": ingest_seconds,
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
            "peak_process_memory_bytes": _peak_memory_bytes(),
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
