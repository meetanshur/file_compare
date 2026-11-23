from __future__ import annotations

import csv
import json
from io import StringIO

from common.comparison_result import ComparisonType, Difference, Severity
from reports.exports import iter_findings_csv, iter_findings_jsonl
from storage.duckdb_store import DuckDBStore


def test_streaming_exports_include_findings_truncation_and_ai_label(tmp_path) -> None:
    with DuckDBStore(tmp_path / "exports.duckdb", max_total_differences=1) as store:
        store.insert_differences(
            [
                Difference(
                    comparison_type=ComparisonType.RAW,
                    location="byte=4",
                    message="changed",
                    severity=Severity.ERROR,
                    value_a="old",
                    value_b="new",
                ),
                Difference(
                    comparison_type=ComparisonType.FIELD,
                    location="row=1",
                    message="field changed",
                ),
            ]
        )

        csv_lines = list(
            iter_findings_csv(
                store,
                explanation_label="AI-generated",
                page_size=1,
            )
        )
        jsonl_lines = list(
            iter_findings_jsonl(
                store,
                explanation_label="AI-generated",
                page_size=1,
            )
        )

    csv_rows = list(csv.DictReader(StringIO("".join(csv_lines))))
    assert len(csv_rows) == 1
    assert csv_rows[0]["comparison_type"] == "raw"
    assert csv_rows[0]["truncated"] == "True"
    assert csv_rows[0]["explanation_label"] == "AI-generated"

    jsonl_records = [json.loads(line) for line in jsonl_lines]
    assert jsonl_records[0] == {
        "record_type": "comparison",
        "truncated": True,
        "explanation_label": "AI-generated",
    }
    assert jsonl_records[1]["record_type"] == "finding"
    assert jsonl_records[1]["finding_id"] == 1
    assert jsonl_records[1]["truncated"] is True
    assert jsonl_records[1]["explanation_label"] == "AI-generated"


def test_exports_are_empty_finding_streams_when_no_findings(tmp_path) -> None:
    with DuckDBStore(tmp_path / "empty.duckdb") as store:
        csv_lines = list(iter_findings_csv(store))
        jsonl_lines = list(iter_findings_jsonl(store))

    assert len(csv_lines) == 1
    assert json.loads(jsonl_lines[0])["record_type"] == "comparison"
