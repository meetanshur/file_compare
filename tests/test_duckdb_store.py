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
