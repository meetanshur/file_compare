from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta, timezone
from io import BytesIO

import pytest

from comparators import MetadataComparator, MetadataStatus, RawComparator
from connectors.base import SourceMetadata


def _metadata(**overrides) -> SourceMetadata:
    values = {
        "uri": "memory://source",
        "source_type": "memory",
        "name": "file.bin",
        "size_bytes": 4,
        "modified_at": datetime(2026, 1, 1, tzinfo=UTC),
        "source_system": "ledger",
        "version": "v1",
    }
    values.update(overrides)
    return SourceMetadata(**values)


def test_metadata_comparator_reports_differences_in_size_system_version_and_time() -> None:
    metadata_a = _metadata()
    metadata_b = _metadata(
        size_bytes=5,
        modified_at=datetime(2026, 1, 2, tzinfo=UTC),
        source_system="billing",
        version="v2",
    )

    comparisons = {
        item.field: item for item in MetadataComparator().compare(metadata_a, metadata_b)
    }

    for field in ("size_bytes", "modified_at", "source_system", "version"):
        assert comparisons[field].status is MetadataStatus.DIFFERENT


def test_metadata_comparator_treats_equivalent_timezones_as_equal() -> None:
    metadata_a = _metadata(
        modified_at=datetime(2026, 1, 1, 12, 0, tzinfo=timezone(timedelta(hours=5, minutes=30)))
    )
    metadata_b = _metadata(modified_at=datetime(2026, 1, 1, 6, 30, tzinfo=UTC))

    modified = next(
        item
        for item in MetadataComparator().compare(metadata_a, metadata_b)
        if item.field == "modified_at"
    )

    assert modified.status is MetadataStatus.EQUAL
    assert modified.value_a == modified.value_b
    assert modified.value_a == datetime(2026, 1, 1, 6, 30, tzinfo=UTC)


def test_missing_metadata_is_unavailable_not_different() -> None:
    metadata_a = _metadata(
        size_bytes=None,
        modified_at=None,
        source_system=None,
        version=None,
        version_id=None,
    )
    metadata_b = _metadata()

    comparisons = list(MetadataComparator().compare(metadata_a, metadata_b))

    assert all(item.status is MetadataStatus.UNAVAILABLE for item in comparisons)
    assert all(
        item.value_a == "unavailable" or item.value_b == "unavailable" for item in comparisons
    )


def test_metadata_comparator_compares_chunked_md5_and_sha256() -> None:
    payload_a = b"data" * 10_000
    payload_b = b"data" * 9_999 + b"last"
    source_a = _TrackedStream(payload_a)
    source_b = _TrackedStream(payload_b)

    comparisons = {
        item.field: item
        for item in MetadataComparator(chunk_size=17).compare(
            _metadata(size_bytes=len(payload_a)),
            _metadata(size_bytes=len(payload_b)),
            source_a=source_a,
            source_b=source_b,
        )
    }

    for algorithm, hash_function in (("md5", hashlib.md5), ("sha256", hashlib.sha256)):
        item = comparisons[algorithm]
        assert item.status is MetadataStatus.DIFFERENT
        assert item.value_a == hash_function(payload_a).hexdigest()
        assert item.value_b == hash_function(payload_b).hexdigest()
    assert max(source_a.requested_sizes) == 17
    assert max(source_b.requested_sizes) == 17


def test_metadata_checksum_loop_checks_job_cancellation() -> None:
    checks = 0

    def cancel_after_two_chunks() -> None:
        nonlocal checks
        checks += 1
        if checks == 2:
            raise RuntimeError("cancelled")

    with pytest.raises(RuntimeError, match="cancelled"):
        list(
            MetadataComparator(chunk_size=4).compare(
                _metadata(),
                _metadata(),
                source_a=BytesIO(b"large data"),
                source_b=BytesIO(b"large data"),
                cancellation_check=cancel_after_two_chunks,
            )
        )

    assert checks == 2


def test_metadata_comparator_reuses_raw_comparison_checksums() -> None:
    payload_a = b"same bytes"
    payload_b = b"same bytes"
    raw_result = RawComparator(chunk_size=3).compare(
        BytesIO(payload_a),
        BytesIO(payload_b),
    )

    comparisons = list(
        MetadataComparator().compare(
            _metadata(size_bytes=len(payload_a)),
            _metadata(size_bytes=len(payload_b)),
            raw_comparison=raw_result,
        )
    )

    checksum_results = {item.field: item for item in comparisons if item.field in {"md5", "sha256"}}
    assert raw_result.identical
    assert raw_result.checksums_a == {
        "md5": hashlib.md5(payload_a, usedforsecurity=False).hexdigest(),
        "sha256": hashlib.sha256(payload_a).hexdigest(),
    }
    assert raw_result.checksums_b == raw_result.checksums_a
    assert checksum_results["md5"].status is MetadataStatus.EQUAL
    assert checksum_results["sha256"].status is MetadataStatus.EQUAL
    assert checksum_results["sha256"].value_a == hashlib.sha256(payload_a).hexdigest()


def test_missing_checksums_without_sources_are_unavailable() -> None:
    comparisons = list(MetadataComparator().compare(_metadata(), _metadata()))

    checksums = [item for item in comparisons if item.field in {"md5", "sha256"}]

    assert len(checksums) == 2
    assert all(item.status is MetadataStatus.UNAVAILABLE for item in checksums)


def test_version_id_is_used_when_explicit_version_is_unavailable() -> None:
    metadata_a = _metadata(version=None, version_id="revision-1")
    metadata_b = _metadata(version=None, version_id="revision-2")

    version = next(
        item
        for item in MetadataComparator().compare(metadata_a, metadata_b)
        if item.field == "version"
    )

    assert version.status is MetadataStatus.DIFFERENT
    assert version.value_a == "revision-1"
    assert version.value_b == "revision-2"


class _TrackedStream(BytesIO):
    def __init__(self, value: bytes) -> None:
        super().__init__(value)
        self.requested_sizes: list[int] = []

    def read(self, size: int = -1) -> bytes:
        self.requested_sizes.append(size)
        return super().read(size)
