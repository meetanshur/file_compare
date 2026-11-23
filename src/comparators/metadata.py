"""Metadata comparison with chunked checksum calculation."""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import BinaryIO

from comparators.raw import RawComparisonResult
from connectors.base import SourceMetadata


class MetadataStatus(StrEnum):
    """Outcome for one metadata field comparison."""

    EQUAL = "equal"
    DIFFERENT = "different"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True)
class MetadataComparison:
    """Comparison outcome for one metadata field."""

    field: str
    status: MetadataStatus
    value_a: object
    value_b: object


class MetadataComparator:
    """Compare source metadata and, when needed, stream checksums in chunks."""

    def __init__(self, *, chunk_size: int = 4 * 1024 * 1024) -> None:
        if chunk_size < 1:
            raise ValueError("chunk_size must be greater than zero")
        self.chunk_size = chunk_size

    def compare(
        self,
        metadata_a: SourceMetadata,
        metadata_b: SourceMetadata,
        *,
        source_a: BinaryIO | None = None,
        source_b: BinaryIO | None = None,
        raw_comparison: RawComparisonResult | None = None,
        cancellation_check: Callable[[], None] | None = None,
    ) -> Iterator[MetadataComparison]:
        """Yield metadata outcomes; missing values are unavailable, not differences.

        ``raw_comparison`` reuses its chunk-computed hashes, avoiding another
        read when raw comparison already consumed the sources.
        """
        fields = (
            ("size_bytes", metadata_a.size_bytes, metadata_b.size_bytes),
            (
                "modified_at",
                _normalize_datetime(metadata_a.modified_at),
                _normalize_datetime(metadata_b.modified_at),
            ),
            ("source_system", metadata_a.source_system, metadata_b.source_system),
            (
                "version",
                metadata_a.version if metadata_a.version is not None else metadata_a.version_id,
                metadata_b.version if metadata_b.version is not None else metadata_b.version_id,
            ),
        )
        for field, value_a, value_b in fields:
            yield _compare_values(field, value_a, value_b)

        checksums_a = _normalized_checksums(metadata_a.checksums)
        checksums_b = _normalized_checksums(metadata_b.checksums)
        if raw_comparison is not None:
            for name, value in raw_comparison.checksums_a.items():
                checksums_a.setdefault(name.lower(), value.lower())
            for name, value in raw_comparison.checksums_b.items():
                checksums_b.setdefault(name.lower(), value.lower())

        missing_a = [name for name in ("md5", "sha256") if name not in checksums_a]
        missing_b = [name for name in ("md5", "sha256") if name not in checksums_b]
        if source_a is not None and missing_a:
            checksums_a.update(_checksums(source_a, missing_a, self.chunk_size, cancellation_check))
        if source_b is not None and missing_b:
            checksums_b.update(_checksums(source_b, missing_b, self.chunk_size, cancellation_check))

        for algorithm in ("md5", "sha256"):
            digest_a = checksums_a.get(algorithm)
            digest_b = checksums_b.get(algorithm)
            yield _compare_values(algorithm, digest_a, digest_b)


def _normalize_datetime(value: datetime | None) -> datetime | None:
    return None if value is None else value.astimezone(UTC)


def _normalized_checksums(checksums: Mapping[str, str]) -> dict[str, str]:
    return {name.lower(): value.lower() for name, value in checksums.items()}


def _checksums(
    source: BinaryIO,
    algorithms: list[str],
    chunk_size: int,
    cancellation_check: Callable[[], None] | None,
) -> dict[str, str]:
    digests = {
        algorithm: (hashlib.md5(usedforsecurity=False) if algorithm == "md5" else hashlib.sha256())
        for algorithm in algorithms
    }
    while chunk := source.read(chunk_size):
        if cancellation_check is not None:
            cancellation_check()
        if not isinstance(chunk, bytes):
            raise TypeError("comparison sources must return bytes")
        for digest in digests.values():
            digest.update(chunk)
    return {algorithm: digest.hexdigest() for algorithm, digest in digests.items()}


def _compare_values(
    field: str, value_a: object | None, value_b: object | None
) -> MetadataComparison:
    if value_a is None or value_b is None:
        return MetadataComparison(
            field=field,
            status=MetadataStatus.UNAVAILABLE,
            value_a="unavailable" if value_a is None else value_a,
            value_b="unavailable" if value_b is None else value_b,
        )
    status = MetadataStatus.EQUAL if value_a == value_b else MetadataStatus.DIFFERENT
    return MetadataComparison(
        field=field,
        status=status,
        value_a=value_a,
        value_b=value_b,
    )
