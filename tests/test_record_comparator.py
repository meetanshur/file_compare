from __future__ import annotations

import io
import json
import time
from collections import Counter
from pathlib import Path

import pytest

from common.comparison_config import ComparisonConfig
from common.comparison_result import ComparisonType
from comparators.records import RecordComparator, parser_record_to_text_mapping
from parsers.delimited import CSVParser
from parsers.structured import StructuredRecord
from parsers.xlsx import XLSXCellValue, XLSXRecord
from parsers.xml import XMLParser
from storage.duckdb_store import DuckDBStore

DATA_DIRECTORY = Path(__file__).resolve().parents[1] / ".test_data"
LARGE_XML_FIXTURES = (
    DATA_DIRECTORY / "xml_100MB_A.xml",
    DATA_DIRECTORY / "xml_100MB_B.xml",
    DATA_DIRECTORY / "xml_100MB_expected.json",
)


def _compare(tmp_path, rows_a, rows_b, **options):
    store = DuckDBStore(
        tmp_path / "records.duckdb",
        batch_size=2,
        max_total_differences=options.pop("max_total_differences", 100),
    )
    result = RecordComparator().compare(
        store,
        rows_a,
        rows_b,
        key_fields=options.pop("key_fields", ["id"]),
        **options,
    )
    return store, result


def test_record_comparison_reports_insert_update_and_delete(tmp_path) -> None:
    store, result = _compare(
        tmp_path,
        [{"id": "keep", "value": "same"}, {"id": "update", "value": "old"}, {"id": "delete"}],
        [{"id": "keep", "value": "same"}, {"id": "update", "value": "new"}, {"id": "insert"}],
    )
    try:
        findings = list(store.iter_differences())
    finally:
        store.close()

    assert (result.records_file_a, result.records_file_b) == (3, 3)
    assert Counter(item.comparison_type for item in findings) == {
        ComparisonType.INSERT: 1,
        ComparisonType.UPDATE: 1,
        ComparisonType.DELETE: 1,
    }
    update = next(item for item in findings if item.comparison_type is ComparisonType.UPDATE)
    assert update.value_a == {"id": "update", "value": "old"}
    assert update.value_b == {"id": "update", "value": "new"}


def test_duplicate_keys_are_reported_and_excluded_from_matching(tmp_path) -> None:
    store, _ = _compare(
        tmp_path,
        [{"id": "dup", "value": "a"}, {"id": "dup", "value": "b"}, {"id": "gone"}],
        [{"id": "dup", "value": "c"}, {"id": "new"}],
    )
    try:
        findings = list(store.iter_differences())
    finally:
        store.close()

    assert Counter(item.comparison_type for item in findings) == {
        ComparisonType.DUPLICATE_KEY: 1,
        ComparisonType.INSERT: 1,
        ComparisonType.DELETE: 1,
    }
    duplicate = next(
        item for item in findings if item.comparison_type is ComparisonType.DUPLICATE_KEY
    )
    assert duplicate.value_a == {"key": [["id", "dup"]], "count": 2}
    assert "excluded from insert/update/delete matching" in duplicate.message


def test_duplicate_keys_on_both_sides_are_reported_separately(tmp_path) -> None:
    store, _ = _compare(
        tmp_path,
        [{"id": "dup"}, {"id": "dup"}],
        [{"id": "dup"}, {"id": "dup"}, {"id": "dup"}],
    )
    try:
        findings = list(store.iter_differences())
    finally:
        store.close()

    assert len(findings) == 2
    assert all(item.comparison_type is ComparisonType.DUPLICATE_KEY for item in findings)
    assert {item.location.split(" ", 1)[0] for item in findings} == {"side=A", "side=B"}
    assert {item.value_a["count"] for item in findings} == {2, 3}


def test_missing_key_field_and_empty_key_configuration_are_errors(tmp_path) -> None:
    store = DuckDBStore(tmp_path / "invalid.duckdb")
    try:
        with pytest.raises(ValueError, match="missing key fields: id"):
            RecordComparator().compare(store, [{"name": "x"}], [], key_fields=["id"])
        assert store.count_records() == 0

        with pytest.raises(ValueError, match="At least one record key field"):
            RecordComparator().compare(store, [], [], key_fields=[])
    finally:
        store.close()


def test_key_matching_is_exact_by_default_and_can_trim_and_ignore_case(tmp_path) -> None:
    exact_store, _ = _compare(
        tmp_path / "exact",
        [{"id": " Key ", "value": "a"}],
        [{"id": "key", "value": "a"}],
    )
    try:
        assert Counter(item.comparison_type for item in exact_store.iter_differences()) == {
            ComparisonType.INSERT: 1,
            ComparisonType.DELETE: 1,
        }
    finally:
        exact_store.close()

    normalized_store, _ = _compare(
        tmp_path / "normalized",
        [{"id": " Key ", "value": "a"}],
        [{"id": "key", "value": "a"}],
        trim_keys=True,
        key_case_sensitive=False,
    )
    try:
        assert list(normalized_store.iter_differences()) == []
        stored_a = next(normalized_store.iter_records())
        assert stored_a.payload["id"] == " Key "
        assert stored_a.record_key == [["id", "key"]]
    finally:
        normalized_store.close()


def test_record_key_and_normalization_are_read_from_config(tmp_path) -> None:
    config = ComparisonConfig(
        record_key=["id"],
        trim_keys=True,
        key_case_sensitive=False,
    )
    store = DuckDBStore(tmp_path / "configured.duckdb")
    try:
        result = RecordComparator().compare(
            store,
            [{"id": " A ", "value": "same"}],
            [{"id": "a", "value": "same"}],
            config=config,
        )
        assert result.records_file_a == result.records_file_b == 1
        assert list(store.iter_differences()) == []
    finally:
        store.close()


def test_composite_record_keys_match_all_configured_fields(tmp_path) -> None:
    store = DuckDBStore(tmp_path / "composite.duckdb")
    try:
        RecordComparator().compare(
            store,
            [{"region": "west", "id": "007", "value": "old"}],
            [{"region": "west", "id": "007", "value": "new"}],
            key_fields=["region", "id"],
        )
        finding = next(store.iter_differences())
    finally:
        store.close()

    assert finding.comparison_type is ComparisonType.UPDATE
    assert finding.value_a == {"id": "007", "region": "west", "value": "old"}
    assert finding.value_b == {"id": "007", "region": "west", "value": "new"}


def test_values_are_preserved_as_text_from_delimited_parser(tmp_path) -> None:
    parser = CSVParser(filename="values.csv")
    rows = list(parser.parse(io.BytesIO(b"id,amount,note\n007,1.10,tail   \n")))
    mapped = parser_record_to_text_mapping(rows[0])

    store, _ = _compare(tmp_path, [mapped], [mapped])
    try:
        stored = next(store.iter_records())
    finally:
        store.close()

    assert stored.payload == {"amount": "1.10", "id": "007", "note": "tail   "}


def test_json_record_keeps_numeric_text_and_distinguishes_null_from_empty() -> None:
    record = StructuredRecord(
        index=1,
        value={"id": "007", "amount": "1.10", "null_value": None, "empty_value": ""},
    )

    assert parser_record_to_text_mapping(record) == {
        "id": "007",
        "amount": "1.10",
        "null_value": "null",
        "empty_value": "",
    }


def test_xml_parser_records_are_flattened_without_changing_text() -> None:
    parser = XMLParser(record_tag="record")
    records = list(
        parser.parse(io.BytesIO(b"<root><record id='007'><value>1.10 </value></record></root>"))
    )

    assert parser_record_to_text_mapping(records[0]) == {
        "@id": "007",
        "value.#text": "1.10 ",
    }


def test_xlsx_parser_record_values_and_sheet_are_retained_as_text() -> None:
    record = XLSXRecord(
        index=1,
        sheet_name="Ledger",
        row_number=2,
        values={
            "id": XLSXCellValue("007", "string", "@"),
            "amount": XLSXCellValue(1.1, "number", "0.00"),
            "formula": XLSXCellValue(
                "=A1+1",
                "formula",
                "General",
                formula="=A1+1",
                cached_value=8,
            ),
        },
    )

    assert parser_record_to_text_mapping(record) == {
        "@sheet_name": "Ledger",
        "id": "007",
        "amount": "1.1",
        "formula": "=A1+1",
        "formula.@cached_value": "8",
    }


def test_record_findings_obey_limit_and_report_truncation(tmp_path) -> None:
    store = DuckDBStore(tmp_path / "limited.duckdb", max_total_differences=1)
    try:
        result = RecordComparator().compare(
            store,
            [{"id": "delete"}],
            [{"id": "insert"}],
            key_fields=["id"],
        )
        findings = list(store.iter_differences())
    finally:
        store.close()

    assert len(findings) == 1
    assert result.differences.truncated is True


def test_record_comparison_checks_cancellation_during_ingestion(tmp_path) -> None:
    checks = 0

    def check_cancelled() -> None:
        nonlocal checks
        checks += 1
        if checks == 3:
            raise RuntimeError("cancelled")

    store = DuckDBStore(tmp_path / "cancelled.duckdb", batch_size=1)
    try:
        with pytest.raises(RuntimeError, match="cancelled"):
            RecordComparator().compare(
                store,
                ({"id": str(index)} for index in range(10)),
                [],
                key_fields=["id"],
                cancellation_check=check_cancelled,
            )
        assert store.count_records() == 1
    finally:
        store.close()


@pytest.mark.skipif(
    not all(path.is_file() for path in LARGE_XML_FIXTURES),
    reason="generated 100 MB XML acceptance fixtures are not available",
)
def test_100mb_xml_records_match_manifest(tmp_path) -> None:
    path_a, path_b, manifest_path = LARGE_XML_FIXTURES
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    def records(path: Path):
        parser = XMLParser(record_tag="record", source_name=path.name)
        with path.open("rb") as source:
            for record in parser.parse(source):
                yield parser_record_to_text_mapping(record)

    store = DuckDBStore(tmp_path / "large-records.duckdb", max_memory_mb=512)
    started = time.perf_counter()
    try:
        result = RecordComparator().compare(
            store,
            records(path_a),
            records(path_b),
            key_fields=["@id"],
        )
        findings = list(store.iter_differences())
        elapsed = time.perf_counter() - started
        expected = manifest["expected_differences"]
        assert result.records_file_a == manifest["record_counts"]["file_a"]
        assert result.records_file_b == manifest["record_counts"]["file_b"]
        assert Counter(item.comparison_type for item in findings) == {
            ComparisonType.INSERT: len(expected["insert"]),
            ComparisonType.UPDATE: len(expected["update"]),
            ComparisonType.DELETE: len(expected["delete"]),
        }
        by_type = {item.comparison_type: item for item in findings}
        assert by_type[ComparisonType.INSERT].value_b["@id"] == expected["insert"][0]["key"]
        assert by_type[ComparisonType.DELETE].value_a["@id"] == expected["delete"][0]["key"]
        update = by_type[ComparisonType.UPDATE]
        assert update.value_a["value.#text"] == expected["update"][0]["value_a"]
        assert update.value_b["value.#text"] == expected["update"][0]["value_b"]
        assert result.differences.truncated is False
    finally:
        store.close()

    mib = (path_a.stat().st_size + path_b.stat().st_size) / (1024 * 1024)
    print(f"record comparison: {elapsed:.3f}s, {mib / elapsed:.2f} MiB/s aggregate")
