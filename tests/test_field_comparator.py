from __future__ import annotations

import io
import json
import time
from collections import Counter
from pathlib import Path

import pytest

from common.comparison_config import ComparisonConfig, Strictness, WhitespaceMode
from common.comparison_result import ComparisonType, Severity
from comparators.fields import FieldComparator
from comparators.records import RecordComparator
from parsers.delimited import CSVParser
from parsers.structured import JSONParser
from parsers.xlsx import XLSXCellValue, XLSXRecord
from parsers.xml import XMLParser
from storage.duckdb_store import DuckDBStore

DATA_DIRECTORY = Path(__file__).resolve().parents[1] / ".test_data"
LARGE_XML_FIXTURES = (
    DATA_DIRECTORY / "xml_100MB_A.xml",
    DATA_DIRECTORY / "xml_100MB_B.xml",
    DATA_DIRECTORY / "xml_100MB_expected.json",
)


def _compare(tmp_path, rows_a, rows_b, *, config=None, **options):
    store = DuckDBStore(
        tmp_path / "fields.duckdb",
        batch_size=2,
        max_total_differences=options.pop("max_total_differences", 100),
    )
    result = FieldComparator().compare(
        store,
        rows_a,
        rows_b,
        config=config or ComparisonConfig(record_key=["id"]),
        **options,
    )
    return store, result


def test_field_comparison_reports_added_removed_renamed_and_reordered_fields(tmp_path) -> None:
    config = ComparisonConfig(record_key=["id"])
    store, _ = _compare(
        tmp_path,
        [{"id": "1", "old_name": "same", "keep": "v", "move": "x"}],
        [{"id": "1", "new_name": "same", "move": "x", "keep": "v", "added": "n"}],
        config=config,
    )
    try:
        findings = list(store.iter_differences(comparison_type=ComparisonType.FIELD))
    finally:
        store.close()

    messages = [item.message for item in findings]
    assert any("renamed from 'old_name' to 'new_name'" in message for message in messages)
    assert any("added in file B" in message and "'added'" in message for message in messages)
    assert any("moved from column" in message and "'keep'" in message for message in messages)
    assert not any("'old_name' was removed" in message for message in messages)


def test_field_comparison_reports_a_removed_field(tmp_path) -> None:
    store, _ = _compare(
        tmp_path,
        [{"id": "1", "keep": "v", "removed": "x"}],
        [{"id": "1", "keep": "v"}],
    )
    try:
        findings = list(store.iter_differences(comparison_type=ComparisonType.FIELD))
    finally:
        store.close()

    removed = next(item for item in findings if "removed from file B" in item.message)
    assert removed.value_a == {"field": "removed", "position": 2}
    assert removed.value_b is None


def test_field_comparison_reports_a_new_field(tmp_path) -> None:
    store, _ = _compare(
        tmp_path,
        [{"id": "1", "keep": "v"}],
        [{"id": "1", "keep": "v", "added": "x"}],
    )
    try:
        findings = list(store.iter_differences(comparison_type=ComparisonType.FIELD))
    finally:
        store.close()

    added = next(item for item in findings if "added in file B" in item.message)
    assert added.value_a is None
    assert added.value_b == {"field": "added", "position": 2}


def test_field_comparison_reports_values_and_json_original_types(tmp_path) -> None:
    parser_a = JSONParser(filename="a.json")
    parser_b = JSONParser(filename="b.json")
    rows_a = list(parser_a.parse(io.BytesIO(b'[{"id":"1","amount":"1.10"}]')))
    rows_b = list(parser_b.parse(io.BytesIO(b'[{"id":"1","amount":1.10}]')))
    store, _ = _compare(
        tmp_path,
        rows_a,
        rows_b,
        config=ComparisonConfig(record_key=["id"]),
    )
    try:
        findings = list(store.iter_differences())
    finally:
        store.close()

    amount = next(item for item in findings if "field=amount" in item.location)
    assert amount.value_a == {
        "value": "1.10",
        "data_type": "string",
        "source_type": "json",
    }
    assert amount.value_b == {
        "value": "1.10",
        "data_type": "number",
        "source_type": "json",
    }


def test_json_boolean_and_null_types_are_retained(tmp_path) -> None:
    rows_a = list(
        JSONParser(filename="a.json").parse(io.BytesIO(b'[{"id":"1","flag":true,"empty":null}]'))
    )
    rows_b = list(
        JSONParser(filename="b.json").parse(io.BytesIO(b'[{"id":"1","flag":"true","empty":""}]'))
    )
    store, _ = _compare(
        tmp_path,
        rows_a,
        rows_b,
        config=ComparisonConfig(record_key=["id"]),
    )
    try:
        findings = list(store.iter_differences())
    finally:
        store.close()

    by_field = {
        item.location.rsplit(" field=", 1)[1]: item
        for item in findings
        if " field=" in item.location
    }
    assert by_field["flag"].value_a["data_type"] == "boolean"
    assert by_field["flag"].value_b["data_type"] == "string"
    assert by_field["empty"].value_a["data_type"] == "null"
    assert by_field["empty"].value_b["data_type"] == "string"


def test_field_comparison_treats_delimited_values_as_text(tmp_path) -> None:
    parser = CSVParser(filename="values.csv")
    rows_a = list(parser.parse(io.BytesIO(b"id,amount\n007,1.10\n")))
    rows_b = list(parser.parse(io.BytesIO(b"id,amount\n007,1.1\n")))
    store, _ = _compare(
        tmp_path,
        rows_a,
        rows_b,
        config=ComparisonConfig(record_key=["id"]),
    )
    try:
        findings = list(store.iter_differences())
    finally:
        store.close()

    amount = next(item for item in findings if "field=amount" in item.location)
    assert amount.value_a["value"] == "1.10"
    assert amount.value_a["data_type"] == "text"
    assert amount.value_b["value"] == "1.1"


def test_field_comparison_reports_xlsx_cell_type(tmp_path) -> None:
    row_a = XLSXRecord(
        index=1,
        sheet_name="Ledger",
        row_number=2,
        values={
            "id": XLSXCellValue("1", "string", "@"),
            "amount": XLSXCellValue("1.10", "string", "@"),
        },
    )
    row_b = XLSXRecord(
        index=1,
        sheet_name="Ledger",
        row_number=2,
        values={
            "id": XLSXCellValue("1", "string", "@"),
            "amount": XLSXCellValue(1.1, "number", "0.00"),
        },
    )
    store, _ = _compare(
        tmp_path,
        [row_a],
        [row_b],
        config=ComparisonConfig(record_key=["id"]),
    )
    try:
        findings = list(store.iter_differences())
    finally:
        store.close()

    amount = next(item for item in findings if "field=amount" in item.location)
    assert amount.value_a == {"value": "1.10", "data_type": "string", "source_type": "xlsx"}
    assert amount.value_b == {"value": "1.1", "data_type": "number", "source_type": "xlsx"}


@pytest.mark.parametrize(
    ("left", "right", "null_equals_empty", "expected_count"),
    [
        ("same", "same", False, 0),
        ("", None, False, 1),
        ("", None, True, 0),
        ("ABC", "abc", False, 1),
        ("ABC", "abc", False, 0),
        (" value ", "value", False, 1),
        (" value ", "value", False, 0),
    ],
)
def test_field_value_rules(tmp_path, left, right, null_equals_empty, expected_count) -> None:
    case_sensitive = not (left == "ABC" and right == "abc" and expected_count == 0)
    whitespace = (
        WhitespaceMode.TRIM
        if left == " value " and right == "value" and expected_count == 0
        else WhitespaceMode.PRESERVE
    )
    config = ComparisonConfig(
        record_key=["id"],
        null_equals_empty=null_equals_empty,
        case_sensitive=case_sensitive,
        whitespace=whitespace,
    )
    store, _ = _compare(
        tmp_path,
        [{"id": "1", "value": left}],
        [{"id": "1", "value": right}],
        config=config,
    )
    try:
        findings = list(store.iter_differences())
    finally:
        store.close()
    value_findings = [item for item in findings if "field=value" in item.location]
    assert len(value_findings) == expected_count


def test_ignored_fields_and_lenient_severity_are_honored(tmp_path) -> None:
    config = ComparisonConfig(
        record_key=["id"],
        ignore_fields=["ignored"],
        strictness=Strictness.LENIENT,
    )
    store, _ = _compare(
        tmp_path,
        [{"id": "1", "ignored": "a", "value": "old"}],
        [{"id": "1", "ignored": "b", "value": "new"}],
        config=config,
    )
    try:
        findings = list(store.iter_differences())
    finally:
        store.close()

    assert len(findings) == 1
    assert "field=value" in findings[0].location
    assert findings[0].severity is Severity.WARNING


def test_field_findings_respect_limit_and_cancellation(tmp_path) -> None:
    store, result = _compare(
        tmp_path / "truncated",
        [{"id": "1", "a": "x", "b": "x"}],
        [{"id": "1", "a": "y", "b": "y"}],
        max_total_differences=1,
    )
    try:
        assert len(list(store.iter_differences())) == 1
        assert result.differences.truncated is True
    finally:
        store.close()

    calls = 0

    def cancel() -> None:
        nonlocal calls
        calls += 1
        if calls == 3:
            raise RuntimeError("cancelled")

    cancelled = DuckDBStore(tmp_path / "cancelled.duckdb")
    try:
        with pytest.raises(RuntimeError, match="cancelled"):
            FieldComparator().compare(
                cancelled,
                ({"id": str(index), "value": "a"} for index in range(10)),
                [],
                config=ComparisonConfig(record_key=["id"]),
                cancellation_check=cancel,
            )
    finally:
        cancelled.close()


@pytest.mark.skipif(
    not all(path.is_file() for path in LARGE_XML_FIXTURES),
    reason="generated 100 MB XML acceptance fixtures are not available",
)
def test_100mb_xml_field_comparison_matches_expected_update(tmp_path, monkeypatch) -> None:
    path_a, path_b, manifest_path = LARGE_XML_FIXTURES
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    timings = {"parse": 0.0, "load": 0.0, "query": 0.0}

    def records(path: Path):
        parser = XMLParser(record_tag="record", source_name=path.name)
        with path.open("rb") as source:
            parsed = parser.parse(source)
            while True:
                parse_started = time.perf_counter()
                try:
                    record = next(parsed)
                except StopIteration:
                    break
                timings["parse"] += time.perf_counter() - parse_started
                yield record

    record_store = DuckDBStore(
        tmp_path / "large-records.duckdb",
        max_memory_mb=256,
        batch_size=10_000,
    )
    store = DuckDBStore(
        tmp_path / "large-fields.duckdb",
        max_memory_mb=256,
        batch_size=10_000,
    )
    original_insert = store.insert_field_records

    def timed_insert(*args, **kwargs):
        parse_before = timings["parse"]
        started = time.perf_counter()
        result = original_insert(*args, **kwargs)
        timings["load"] += time.perf_counter() - started - (timings["parse"] - parse_before)
        return result

    original_compare = store.iter_field_comparison_differences

    def timed_compare(**kwargs):
        started = time.perf_counter()
        try:
            yield from original_compare(**kwargs)
        finally:
            timings["query"] += time.perf_counter() - started

    original_indexes = store.create_field_record_indexes

    def timed_indexes():
        started = time.perf_counter()
        try:
            return original_indexes()
        finally:
            timings["load"] += time.perf_counter() - started

    monkeypatch.setattr(store, "insert_field_records", timed_insert)
    monkeypatch.setattr(store, "iter_field_comparison_differences", timed_compare)
    monkeypatch.setattr(store, "create_field_record_indexes", timed_indexes)
    started = time.perf_counter()
    try:
        record_started = time.perf_counter()
        try:
            RecordComparator().compare(
                record_store,
                records(path_a),
                records(path_b),
                key_fields=["@id"],
            )
            record_findings = list(record_store.iter_differences())
        finally:
            record_store.close()
        record_elapsed = time.perf_counter() - record_started
        timings["parse"] = 0.0
        result = FieldComparator().compare(
            store,
            records(path_a),
            records(path_b),
            config=ComparisonConfig(record_key=["@id"]),
        )
        field_findings = list(store.iter_differences())
        elapsed = time.perf_counter() - started
        expected = manifest["expected_differences"]["update"][0]
        record_findings = [
            item
            for item in record_findings
            if item.comparison_type
            in {
                ComparisonType.INSERT,
                ComparisonType.UPDATE,
                ComparisonType.DELETE,
            }
        ]
        update = next(
            item for item in record_findings if item.comparison_type is ComparisonType.UPDATE
        )
        value_change = next(item for item in field_findings if "field=value.#text" in item.location)
        assert result.records_file_a == manifest["record_counts"]["file_a"]
        assert result.records_file_b == manifest["record_counts"]["file_b"]
        assert Counter(item.comparison_type for item in record_findings) == {
            ComparisonType.INSERT: len(manifest["expected_differences"]["insert"]),
            ComparisonType.UPDATE: len(manifest["expected_differences"]["update"]),
            ComparisonType.DELETE: len(manifest["expected_differences"]["delete"]),
        }
        assert len(field_findings) == 1
        assert update.value_a["@id"] == expected["key"]
        assert update.value_a["value.#text"] == expected["value_a"]
        assert update.value_b["value.#text"] == expected["value_b"]
        assert value_change.value_a["value"] == expected["value_a"]
        assert value_change.value_b["value"] == expected["value_b"]
    finally:
        store.close()
        record_store.close()

    total_mib = (path_a.stat().st_size + path_b.stat().st_size) / (1024 * 1024)
    print(
        f"record+field comparison: {elapsed:.3f}s, {total_mib / elapsed:.2f} MiB/s aggregate "
        f"(record={record_elapsed:.3f}s); "
        f"parse={timings['parse']:.3f}s, DuckDB load={timings['load']:.3f}s, "
        f"comparison query={timings['query']:.3f}s"
    )
