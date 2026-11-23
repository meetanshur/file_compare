import json

import pytest

from tools.benchmark_large_files import main, run_benchmark
from tools.generate_test_files import generate_pair


@pytest.mark.parametrize("file_format", ["xml", "csv"])
def test_benchmark_streams_records_and_validates_manifest(tmp_path, file_format) -> None:
    fixture_directory = tmp_path / ".test_data"
    pair = generate_pair(
        fixture_directory,
        file_format,
        8_192,
        size_label="benchmark",
        chunk_size=113,
        row_width=128,
    )

    report = run_benchmark(
        pair.manifest,
        max_memory_mb=512,
        batch_size=7,
    )

    assert report["benchmark_scope"].startswith("streaming parse and DuckDB record ingestion")
    assert report["settings"]["max_memory_mb"] == 512
    assert report["settings"]["duckdb_memory_mb"] == 256
    assert report["index_creation_seconds"] >= 0
    assert report["validation"]["hashes_match_manifest"] is True
    assert report["validation"]["record_counts_match_manifest"] is True
    assert report["validation"]["peak_memory_within_budget"] is True
    assert report["validation"]["difference_comparison_performed"] is False
    assert [item["records_ingested"] for item in report["files"]] == [
        pair.record_count,
        pair.record_count,
    ]
    assert report["totals"]["bytes_ingested"] == 2 * pair.size_bytes
    assert report["totals"]["peak_process_memory_bytes"] > 0
    assert report["totals"]["parser_only_seconds"] > 0


def test_benchmark_rejects_manifest_outside_test_data(tmp_path) -> None:
    manifest = tmp_path / "manifest.json"
    manifest.write_text("{}", encoding="utf-8")

    with pytest.raises(ValueError, match="ignored .test_data directory"):
        run_benchmark(manifest)


def test_benchmark_cli_writes_report_only_into_test_data(tmp_path) -> None:
    fixture_directory = tmp_path / ".test_data"
    pair = generate_pair(
        fixture_directory,
        "csv",
        8_192,
        size_label="cli",
        row_width=128,
    )

    assert (
        main(
            [
                "--manifest",
                str(pair.manifest),
                "--max-memory-mb",
                "32",
                "--batch-size",
                "10",
            ]
        )
        == 0
    )

    report_path = fixture_directory / "csv_cli_expected_benchmark.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["validation"]["record_counts_match_manifest"] is True
    assert report["settings"]["duckdb_memory_mb"] == 16


def test_benchmark_rejects_hash_mismatch(tmp_path) -> None:
    fixture_directory = tmp_path / ".test_data"
    pair = generate_pair(
        fixture_directory,
        "csv",
        8_192,
        size_label="tampered",
        row_width=128,
    )
    with pair.file_a.open("r+b") as stream:
        stream.write(b"X")

    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        run_benchmark(pair.manifest)
