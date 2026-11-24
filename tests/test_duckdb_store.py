from decimal import Decimal
from uuid import uuid4

import pytest

from common.comparison_result import ComparisonType, Difference, Severity
from storage.duckdb_store import DuckDBStore, RecordSide


def _difference(location: str) -> Difference:
    return Difference(
        comparison_type=ComparisonType.FIELD,
        location=location,
        message="Values differ",
        severity=Severity.ERROR,
        value_a=Decimal("10.00"),
        value_b=Decimal("11.00"),
    )


def test_records_are_persisted_with_keys_in_bounded_batches(tmp_path) -> None:
    with DuckDBStore(tmp_path / "records.duckdb", batch_size=2) as store:
        inserted = store.insert_records(
            RecordSide.FILE_A,
            ({"id": index, "amount": Decimal("12.50")} for index in range(5)),
            key_fields=["id"],
        )

        assert inserted == 5
        assert store.count_records(RecordSide.FILE_A) == 5
        assert store.count_records(RecordSide.FILE_B) == 0
        records = list(store.iter_records(RecordSide.FILE_A, batch_size=2))

    assert len(records) == 5
    assert records[0].record_key == [["id", 0]]
    assert records[0].payload == {"amount": "12.50", "id": 0}


def test_native_csv_ingestion_uses_duckdb_and_persists_keys(tmp_path) -> None:
    csv_path = tmp_path / "records.csv"
    csv_path.write_text("id,value\n1,first\n2,\n", encoding="utf-8")

    with DuckDBStore(tmp_path / "native-csv.duckdb") as store:
        inserted = store.insert_csv_file(RecordSide.FILE_A, csv_path, key_fields=["id"])
        records = list(store.iter_records(RecordSide.FILE_A))

    assert inserted == 2
    assert records[0].record_key == [["id", "1"]]
    assert records[0].payload == {"id": "1", "value": "first"}
    assert records[1].payload == {"id": "2", "value": ""}


def test_native_csv_ingestion_preserves_field_text_and_quoted_values(tmp_path) -> None:
    csv_path = tmp_path / "exact-values.csv"
    csv_path.write_text(
        'id,amount,trailing,quoted\n007,1.10,"tail   ","hello, ""world"""\n',
        encoding="utf-8",
        newline="",
    )

    with DuckDBStore(tmp_path / "exact-values.duckdb") as store:
        inserted = store.insert_csv_file(RecordSide.FILE_A, csv_path, key_fields=["id"])
        records = list(store.iter_records(RecordSide.FILE_A))

    assert inserted == 1
    assert records[0].record_key == [["id", "007"]]
    assert records[0].payload == {
        "id": "007",
        "amount": "1.10",
        "trailing": "tail   ",
        "quoted": 'hello, "world"',
    }


def test_duckdb_uses_configured_memory_limit_and_temp_directory(tmp_path) -> None:
    temp_directory = tmp_path / "duckdb-spill"
    with DuckDBStore(
        tmp_path / "resources.duckdb",
        max_memory_mb=32,
        temp_directory=temp_directory,
    ) as store:
        memory_limit, configured_temp_directory, threads, preserve_insertion_order = (
            store._require_connection()
            .execute(
                "SELECT current_setting('memory_limit'), current_setting('temp_directory'), "
                "current_setting('threads'), current_setting('preserve_insertion_order')"
            )
            .fetchone()
        )

    assert memory_limit == "32.0 MiB"
    assert configured_temp_directory == str(temp_directory)
    assert threads == 4
    assert preserve_insertion_order is False
    assert temp_directory.is_dir()


def test_duckdb_rejects_non_positive_thread_count(tmp_path) -> None:
    with pytest.raises(ValueError, match="duckdb_threads must be greater than zero"):
        DuckDBStore(tmp_path / "invalid.duckdb", duckdb_threads=0)


def test_duckdb_rejects_non_positive_memory_limit(tmp_path) -> None:
    with pytest.raises(ValueError, match="max_memory_mb must be greater than zero"):
        DuckDBStore(tmp_path / "invalid.duckdb", max_memory_mb=0)


def test_records_require_configured_key_fields(tmp_path) -> None:
    with DuckDBStore(tmp_path / "records.duckdb") as store:
        with pytest.raises(ValueError, match="missing key fields: id"):
            store.insert_records(RecordSide.FILE_A, [{"name": "missing"}], key_fields=["id"])

        assert store.count_records() == 0


def test_findings_are_paginated_filtered_and_truncated_at_limit(tmp_path) -> None:
    consumed: list[str] = []

    def findings():
        for location in ("one", "two", "three"):
            consumed.append(location)
            yield _difference(location)

    database_path = tmp_path / "results.duckdb"
    job_id = uuid4()
    with DuckDBStore(
        database_path,
        job_id=job_id,
        max_total_differences=2,
        batch_size=10,
    ) as store:
        result = store.insert_differences(findings())

        assert result.inserted == 2
        assert result.total_persisted == 2
        assert result.truncated is True
        assert store.truncated is True
        assert consumed == ["one", "two"]
        assert store.count_differences(ComparisonType.FIELD) == 2
        assert [item.location for item in store.iter_differences(offset=1, limit=1)] == ["two"]

    with DuckDBStore(database_path, job_id=job_id, max_total_differences=2) as reopened:
        assert reopened.truncated is True
        assert reopened.count_differences() == 2


def test_zero_difference_limit_means_unlimited(tmp_path) -> None:
    with DuckDBStore(tmp_path / "unlimited.duckdb", max_total_differences=0) as store:
        result = store.insert_differences(_difference(str(index)) for index in range(3))

        assert result.inserted == 3
        assert result.total_persisted == 3
        assert result.truncated is False
        assert [item.location for item in store.iter_differences()] == ["0", "1", "2"]
