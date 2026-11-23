"""Streaming CSV and JSONL exports for persisted comparison findings."""

from __future__ import annotations

import csv
import json
from collections.abc import Iterator
from io import StringIO
from typing import Any

from common.comparison_result import Difference
from storage.duckdb_store import DuckDBStore

_CSV_FIELDS = (
    "finding_id",
    "comparison_type",
    "location",
    "message",
    "severity",
    "value_a",
    "value_b",
    "truncated",
    "explanation_label",
)


def iter_findings_csv(
    store: DuckDBStore,
    *,
    explanation_label: str | None = None,
    page_size: int = 1_000,
) -> Iterator[str]:
    """Yield a CSV header and rows in bounded pages from DuckDB."""
    _validate_page_size(page_size)
    yield _csv_row(_CSV_FIELDS)
    for finding_id, difference in _iter_all(store, page_size):
        yield _csv_row(
            _finding_values(
                finding_id,
                difference,
                store.truncated,
                explanation_label,
            )
        )


def iter_findings_jsonl(
    store: DuckDBStore,
    *,
    explanation_label: str | None = None,
    page_size: int = 1_000,
) -> Iterator[str]:
    """Yield one metadata line followed by finding objects as JSONL."""
    _validate_page_size(page_size)
    yield _json_line(
        {
            "record_type": "comparison",
            "truncated": store.truncated,
            "explanation_label": explanation_label,
        }
    )
    for finding_id, difference in _iter_all(store, page_size):
        yield _json_line(
            {
                "record_type": "finding",
                **_finding_dict(
                    finding_id,
                    difference,
                    store.truncated,
                    explanation_label,
                ),
            }
        )


def _iter_all(store: DuckDBStore, page_size: int):
    offset = 0
    while True:
        page = list(store.iter_differences_with_ids(offset=offset, limit=page_size))
        if not page:
            return
        yield from page
        offset += len(page)
        if len(page) < page_size:
            return


def _finding_values(
    finding_id: int,
    finding: Difference,
    truncated: bool,
    explanation_label: str | None,
) -> tuple[Any, ...]:
    value = _finding_dict(finding_id, finding, truncated, explanation_label)
    return tuple(value[field] for field in _CSV_FIELDS)


def _finding_dict(
    finding_id: int,
    finding: Difference,
    truncated: bool,
    explanation_label: str | None,
) -> dict[str, Any]:
    return {
        "finding_id": finding_id,
        "comparison_type": finding.comparison_type.value,
        "location": finding.location,
        "message": finding.message,
        "severity": finding.severity.value,
        "value_a": _encode_value(finding.value_a),
        "value_b": _encode_value(finding.value_b),
        "truncated": truncated,
        "explanation_label": explanation_label,
    }


def _encode_value(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, default=str)


def _csv_row(values: tuple[Any, ...]) -> str:
    buffer = StringIO(newline="")
    csv.writer(buffer, lineterminator="\n").writerow(values)
    return buffer.getvalue()


def _json_line(value: dict[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=False, default=str, separators=(",", ":")) + "\n"


def _validate_page_size(page_size: int) -> None:
    if page_size < 1 or page_size > 10_000:
        raise ValueError("page_size must be between 1 and 10000")
