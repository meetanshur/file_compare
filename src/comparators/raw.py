"""Streaming byte- and character-level comparison."""

from __future__ import annotations

import codecs
import hashlib
import tempfile
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import BinaryIO

from parsers.detection import DEFAULT_SAMPLE_SIZE_BYTES, detect_file

_VALUE_PREVIEW_BYTES = 256


class RawDifferenceKind(StrEnum):
    """Edit category for one resynchronized raw difference."""

    INSERT = "insert"
    DELETE = "delete"
    REPLACED = "replaced"


class RawComparisonStatus(StrEnum):
    """Completion state of a raw comparison result."""

    IDENTICAL = "identical"
    PENDING = "pending"
    COMPLETE = "complete"
    TRUNCATED = "truncated"


@dataclass(frozen=True)
class RawDifference:
    """One edit region with byte ranges and decoded-text positions."""

    kind: RawDifferenceKind
    offset_a: int
    offset_b: int
    length_a: int
    length_b: int
    character_offset_a: int
    character_offset_b: int
    line_a: int
    column_a: int
    line_b: int
    column_b: int
    value_a: bytes
    value_b: bytes
    text_a: str
    text_b: str


class RawComparisonResult:
    """One-shot streaming result with completion, identity, and truncation state."""

    def __init__(
        self,
        differences: Iterator[RawDifference] | None = None,
        *,
        identical: bool = False,
        max_total_differences: int = 0,
        checksums_a: dict[str, str] | None = None,
        checksums_b: dict[str, str] | None = None,
        close_resources: Callable[[], None] | None = None,
    ) -> None:
        self.is_identical = identical
        self.checksums_a = checksums_a or {}
        self.checksums_b = checksums_b or {}
        self.max_total_differences = max_total_differences
        self.difference_count = 0
        self.truncated = False
        self.message = "Files are identical." if identical else ""
        self.status = RawComparisonStatus.IDENTICAL if identical else RawComparisonStatus.PENDING
        self._differences = differences
        self._close_resources = close_resources or (lambda: None)
        self._iterated = False
        self._closed = identical
        if identical:
            self._close_resources()

    @property
    def identical(self) -> bool:
        """Whether equal sizes and SHA-256 digests proved the sources identical."""
        return self.is_identical

    def __iter__(self) -> Iterator[RawDifference]:
        if self._iterated:
            raise RuntimeError("Raw comparison results can only be iterated once")
        self._iterated = True
        if self.is_identical:
            return iter(())
        return self._iterate()

    def _iterate(self) -> Iterator[RawDifference]:
        assert self._differences is not None
        try:
            for difference in self._differences:
                if (
                    self.max_total_differences > 0
                    and self.difference_count >= self.max_total_differences
                ):
                    self.truncated = True
                    self.status = RawComparisonStatus.TRUNCATED
                    self.message = (
                        f"Comparison truncated after {self.difference_count} differences; "
                        "additional differences were not processed."
                    )
                    return
                self.difference_count += 1
                yield difference
            self.status = RawComparisonStatus.COMPLETE
            self.message = f"Comparison complete: {self.difference_count} differences."
        finally:
            self.close()

    def close(self) -> None:
        """Close temporary storage if iteration stops before completion."""
        if not self._closed:
            self._closed = True
            if self._differences is not None:
                close_generator = getattr(self._differences, "close", None)
                if close_generator is not None:
                    close_generator()
            self._close_resources()

    def __enter__(self) -> RawComparisonResult:
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        self.close()

    def __del__(self) -> None:
        self.close()


@dataclass
class _PreparedSource:
    stream: BinaryIO
    size: int
    sha256: bytes
    md5: bytes
    sample: bytes
    encoding: str

    def close(self) -> None:
        self.stream.close()


@dataclass
class _TextPosition:
    offset: int = 0
    line: int = 1
    column: int = 1
    pending_carriage_return: bool = False

    def current_position(self) -> tuple[int, int, int]:
        if self.pending_carriage_return:
            return self.offset, self.line + 1, 1
        return self.offset, self.line, self.column

    def advance(self, text: str) -> None:
        if not text:
            return
        self.offset += len(text)
        if self.pending_carriage_return:
            self.line += 1
            self.column = 1
            self.pending_carriage_return = False
            if text.startswith("\n"):
                text = text[1:]
                if not text:
                    return

        trailing_carriage_return = text.endswith("\r")
        count_text = text[:-1] if trailing_carriage_return else text
        line_breaks = count_text.count("\r") + count_text.count("\n") - count_text.count("\r\n")
        if line_breaks:
            self.line += line_breaks
            last_break = max(count_text.rfind("\r"), count_text.rfind("\n"))
            break_end = last_break + 1
            if count_text[last_break : last_break + 2] == "\r\n":
                break_end += 1
            self.column = len(count_text) - break_end + 1
        else:
            self.column += len(text) - int(trailing_carriage_return)
        self.pending_carriage_return = trailing_carriage_return


class RawComparator:
    """Compare two binary streams with bounded-memory re-synchronization."""

    def __init__(
        self,
        *,
        encoding_a: str | None = None,
        encoding_b: str | None = None,
        filename_a: str | Path | None = None,
        filename_b: str | Path | None = None,
        chunk_size: int = 4 * 1024 * 1024,
        sample_size_bytes: int = DEFAULT_SAMPLE_SIZE_BYTES,
        resync_window_bytes: int = 64 * 1024,
        sync_bytes: int = 32,
        max_total_differences: int = 0,
    ) -> None:
        if chunk_size < 1:
            raise ValueError("chunk_size must be greater than zero")
        if sample_size_bytes < 1:
            raise ValueError("sample_size_bytes must be greater than zero")
        if resync_window_bytes < 1:
            raise ValueError("resync_window_bytes must be greater than zero")
        if sync_bytes < 1:
            raise ValueError("sync_bytes must be greater than zero")
        if max_total_differences < 0:
            raise ValueError("max_total_differences must be zero or greater")
        self.encoding_a = encoding_a
        self.encoding_b = encoding_b
        self.filename_a = str(filename_a) if filename_a is not None else None
        self.filename_b = str(filename_b) if filename_b is not None else None
        self.chunk_size = chunk_size
        self.sample_size_bytes = sample_size_bytes
        self.resync_window_bytes = resync_window_bytes
        self.sync_bytes = sync_bytes
        self.max_total_differences = max_total_differences

    def compare(
        self,
        source_a: BinaryIO,
        source_b: BinaryIO,
        *,
        cancellation_check: Callable[[], None] | None = None,
    ) -> RawComparisonResult:
        """Hash and stage sources, then return a lazy stream of raw edit findings.

        Temporary files permit bounded-memory hashing and random lookahead even
        when either source is non-seekable.
        """
        prepared_a: _PreparedSource | None = None
        prepared_b: _PreparedSource | None = None
        try:
            prepared_a = _prepare_source(
                source_a,
                self.chunk_size,
                self.sample_size_bytes,
                self.encoding_a,
                self.filename_a,
                cancellation_check,
            )
            prepared_b = _prepare_source(
                source_b,
                self.chunk_size,
                self.sample_size_bytes,
                self.encoding_b,
                self.filename_b,
                cancellation_check,
            )
            if prepared_a.size == prepared_b.size and prepared_a.sha256 == prepared_b.sha256:
                checksums_a = _checksum_values(prepared_a)
                checksums_b = _checksum_values(prepared_b)
                prepared_a.close()
                prepared_b.close()
                return RawComparisonResult(
                    identical=True,
                    max_total_differences=self.max_total_differences,
                    checksums_a=checksums_a,
                    checksums_b=checksums_b,
                )

            differences = self._iter_differences(
                prepared_a,
                prepared_b,
                cancellation_check,
            )
            return RawComparisonResult(
                differences,
                max_total_differences=self.max_total_differences,
                checksums_a=_checksum_values(prepared_a),
                checksums_b=_checksum_values(prepared_b),
                close_resources=lambda: _close_sources(prepared_a, prepared_b),
            )
        except Exception:
            if prepared_a is not None:
                prepared_a.close()
            if prepared_b is not None:
                prepared_b.close()
            raise

    def _iter_differences(
        self,
        source_a: _PreparedSource,
        source_b: _PreparedSource,
        cancellation_check: Callable[[], None] | None,
    ) -> Iterator[RawDifference]:
        decoder_a = _make_decoder(source_a.encoding)
        decoder_b = _make_decoder(source_b.encoding)
        position_a = _TextPosition()
        position_b = _TextPosition()
        offset_a = 0
        offset_b = 0

        while offset_a < source_a.size or offset_b < source_b.size:
            _check_cancelled(cancellation_check)
            if offset_a == source_a.size:
                length_b = min(self.resync_window_bytes, source_b.size - offset_b)
                yield _make_difference(
                    RawDifferenceKind.INSERT,
                    source_a,
                    source_b,
                    offset_a,
                    offset_b,
                    0,
                    length_b,
                    decoder_a,
                    decoder_b,
                    position_a,
                    position_b,
                    self.chunk_size,
                    cancellation_check,
                )
                offset_b += length_b
                continue
            if offset_b == source_b.size:
                length_a = min(self.resync_window_bytes, source_a.size - offset_a)
                yield _make_difference(
                    RawDifferenceKind.DELETE,
                    source_a,
                    source_b,
                    offset_a,
                    offset_b,
                    length_a,
                    0,
                    decoder_a,
                    decoder_b,
                    position_a,
                    position_b,
                    self.chunk_size,
                    cancellation_check,
                )
                offset_a += length_a
                continue

            block_a = _read_at(
                source_a.stream, offset_a, min(self.chunk_size, source_a.size - offset_a)
            )
            block_b = _read_at(
                source_b.stream, offset_b, min(self.chunk_size, source_b.size - offset_b)
            )
            common_length = _common_prefix_length(
                block_a,
                block_b,
                cancellation_check,
            )
            if common_length:
                _advance_text(decoder_a, position_a, block_a[:common_length])
                _advance_text(decoder_b, position_b, block_b[:common_length])
                offset_a += common_length
                offset_b += common_length
                if common_length == len(block_a) and common_length == len(block_b):
                    continue
                if offset_a == source_a.size or offset_b == source_b.size:
                    continue
                block_a = block_a[common_length:]
                block_b = block_b[common_length:]

            _check_cancelled(cancellation_check)
            if offset_a == source_a.size or offset_b == source_b.size:
                continue
            edit = _find_resynchronization(
                source_a,
                source_b,
                offset_a,
                offset_b,
                self.resync_window_bytes,
                self.sync_bytes,
                cancellation_check,
            )
            kind, length_a, length_b = edit
            yield _make_difference(
                kind,
                source_a,
                source_b,
                offset_a,
                offset_b,
                length_a,
                length_b,
                decoder_a,
                decoder_b,
                position_a,
                position_b,
                self.chunk_size,
                cancellation_check,
            )
            offset_a += length_a
            offset_b += length_b


def _prepare_source(
    source: BinaryIO,
    chunk_size: int,
    sample_size_bytes: int,
    encoding_override: str | None,
    filename: str | None,
    cancellation_check: Callable[[], None] | None,
) -> _PreparedSource:
    staged = tempfile.TemporaryFile(mode="w+b")
    digest = hashlib.sha256()
    md5_digest = hashlib.md5(usedforsecurity=False)
    size = 0
    sample = bytearray()
    try:
        while True:
            _check_cancelled(cancellation_check)
            chunk = _read_bytes(source, chunk_size)
            if not chunk:
                break
            staged.write(chunk)
            digest.update(chunk)
            md5_digest.update(chunk)
            size += len(chunk)
            if len(sample) < sample_size_bytes:
                sample.extend(chunk[: sample_size_bytes - len(sample)])
        encoding = _choose_encoding(encoding_override, bytes(sample), filename)
        staged.seek(0)
        return _PreparedSource(
            staged,
            size,
            digest.digest(),
            md5_digest.digest(),
            bytes(sample),
            encoding,
        )
    except Exception:
        staged.close()
        raise


def _find_resynchronization(
    source_a: _PreparedSource,
    source_b: _PreparedSource,
    offset_a: int,
    offset_b: int,
    window_size: int,
    sync_bytes: int,
    cancellation_check: Callable[[], None] | None,
) -> tuple[RawDifferenceKind, int, int]:
    available_a = source_a.size - offset_a
    available_b = source_b.size - offset_b
    read_size = min(max(available_a, available_b), window_size + sync_bytes)
    lookahead_a = _read_at(source_a.stream, offset_a, min(available_a, read_size))
    lookahead_b = _read_at(source_b.stream, offset_b, min(available_b, read_size))
    max_shift = min(window_size, max(available_a, available_b))

    for shift in range(1, max_shift + 1):
        _check_cancelled(cancellation_check)
        if _has_sync(
            lookahead_a,
            0,
            lookahead_b,
            shift,
            sync_bytes,
            available_a,
            available_b - shift,
        ):
            return RawDifferenceKind.INSERT, 0, shift
        if _has_sync(
            lookahead_a,
            shift,
            lookahead_b,
            0,
            sync_bytes,
            available_a - shift,
            available_b,
        ):
            return RawDifferenceKind.DELETE, shift, 0
        if _has_sync(
            lookahead_a,
            shift,
            lookahead_b,
            shift,
            sync_bytes,
            available_a - shift,
            available_b - shift,
        ):
            return RawDifferenceKind.REPLACED, shift, shift

    return (
        RawDifferenceKind.REPLACED,
        min(window_size, available_a),
        min(window_size, available_b),
    )


def _has_sync(
    data_a: bytes,
    start_a: int,
    data_b: bytes,
    start_b: int,
    sync_bytes: int,
    remaining_a: int,
    remaining_b: int,
) -> bool:
    common_available = min(len(data_a) - start_a, len(data_b) - start_b)
    if common_available >= sync_bytes:
        return data_a[start_a : start_a + sync_bytes] == data_b[start_b : start_b + sync_bytes]
    return (
        remaining_a == remaining_b
        and common_available == remaining_a
        and common_available == remaining_b
        and common_available > 0
        and data_a[start_a : start_a + common_available]
        == data_b[start_b : start_b + common_available]
    )


def _make_difference(
    kind: RawDifferenceKind,
    source_a: _PreparedSource,
    source_b: _PreparedSource,
    offset_a: int,
    offset_b: int,
    length_a: int,
    length_b: int,
    decoder_a,
    decoder_b,
    position_a: _TextPosition,
    position_b: _TextPosition,
    chunk_size: int,
    cancellation_check: Callable[[], None] | None,
) -> RawDifference:
    start_a = position_a.current_position()
    start_b = position_b.current_position()
    bytes_a = _read_at(source_a.stream, offset_a, length_a)
    bytes_b = _read_at(source_b.stream, offset_b, length_b)
    text_a = _advance_span(
        source_a.stream,
        offset_a,
        length_a,
        decoder_a,
        position_a,
        chunk_size,
        cancellation_check,
    )
    text_b = _advance_span(
        source_b.stream,
        offset_b,
        length_b,
        decoder_b,
        position_b,
        chunk_size,
        cancellation_check,
    )
    return RawDifference(
        kind=kind,
        offset_a=offset_a,
        offset_b=offset_b,
        length_a=length_a,
        length_b=length_b,
        character_offset_a=start_a[0],
        character_offset_b=start_b[0],
        line_a=start_a[1],
        column_a=start_a[2],
        line_b=start_b[1],
        column_b=start_b[2],
        value_a=bytes_a,
        value_b=bytes_b,
        text_a=text_a[:_VALUE_PREVIEW_BYTES],
        text_b=text_b[:_VALUE_PREVIEW_BYTES],
    )


def _advance_text(decoder, position: _TextPosition, data: bytes) -> str:
    decoded = decoder.decode(data, final=False)
    position.advance(decoded)
    return decoded


def _advance_span(
    source: BinaryIO,
    offset: int,
    length: int,
    decoder,
    position: _TextPosition,
    chunk_size: int,
    cancellation_check: Callable[[], None] | None,
) -> str:
    preview = ""
    while length:
        _check_cancelled(cancellation_check)
        chunk = _read_at(source, offset, min(length, chunk_size))
        if not chunk:
            break
        decoded = _advance_text(decoder, position, chunk)
        if len(preview) < _VALUE_PREVIEW_BYTES:
            preview += decoded[: _VALUE_PREVIEW_BYTES - len(preview)]
        offset += len(chunk)
        length -= len(chunk)
    return preview


def _common_prefix_length(
    data_a: bytes,
    data_b: bytes,
    cancellation_check: Callable[[], None] | None,
) -> int:
    limit = min(len(data_a), len(data_b))
    _check_cancelled(cancellation_check)
    if data_a[:limit] == data_b[:limit]:
        return limit
    for index in range(limit):
        if index % (64 * 1024) == 0:
            _check_cancelled(cancellation_check)
        if data_a[index] != data_b[index]:
            return index
    return limit


def _read_at(source: BinaryIO, offset: int, length: int) -> bytes:
    source.seek(offset)
    chunks = bytearray()
    while len(chunks) < length:
        part = _read_bytes(source, length - len(chunks))
        if not part:
            break
        chunks.extend(part)
    return bytes(chunks)


def _read_bytes(source: BinaryIO, size: int) -> bytes:
    chunk = source.read(size)
    if not isinstance(chunk, bytes):
        raise TypeError("comparison sources must return bytes")
    return chunk


def _choose_encoding(
    override: str | None,
    sample: bytes,
    filename: str | None,
) -> str:
    if override is not None:
        try:
            return codecs.lookup(override).name
        except LookupError as exc:
            raise ValueError(f"Unknown text encoding {override!r}") from exc
    detection = detect_file(sample, filename, sample_size_bytes=len(sample) or 1)
    return detection.encoding or "latin-1"


def _make_decoder(encoding: str):
    try:
        return codecs.getincrementaldecoder(encoding)(errors="surrogateescape")
    except LookupError as exc:
        raise ValueError(f"Unknown text encoding {encoding!r}") from exc


def _check_cancelled(cancellation_check: Callable[[], None] | None) -> None:
    if cancellation_check is not None:
        cancellation_check()


def _close_sources(source_a: _PreparedSource, source_b: _PreparedSource) -> None:
    source_a.close()
    source_b.close()


def _checksum_values(source: _PreparedSource) -> dict[str, str]:
    return {
        "md5": source.md5.hex(),
        "sha256": source.sha256.hex(),
    }
