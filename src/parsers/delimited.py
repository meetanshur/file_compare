"""Streaming CSV and TSV parsing with format and encoding detection."""

from __future__ import annotations

import csv
import io
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

from parsers.base import Parser
from parsers.detection import (
    DEFAULT_SAMPLE_SIZE_BYTES,
    FileFormat,
    detect_file,
)


class DelimitedParseError(ValueError):
    """A delimited source has invalid configuration or malformed text."""


@dataclass(frozen=True)
class ParseWarning:
    """A recoverable issue found while parsing one input row."""

    code: str
    message: str
    row_number: int


@dataclass(frozen=True)
class DelimitedRecord:
    """A delimited row represented as text values without type coercion."""

    index: int
    values: Mapping[str, str]
    extra_values: tuple[str, ...] = ()
    warnings: tuple[ParseWarning, ...] = ()


class DelimitedTextParser(Parser[DelimitedRecord]):
    """Incrementally parse CSV or TSV, preserving parsed field text."""

    def __init__(
        self,
        *,
        filename: str | Path | None = None,
        format_override: FileFormat | str | None = None,
        encoding_override: str | None = None,
        delimiter: str | None = None,
        has_header: bool = True,
        sample_size_bytes: int = DEFAULT_SAMPLE_SIZE_BYTES,
    ) -> None:
        if delimiter is not None and len(delimiter) != 1:
            raise ValueError("delimiter must be exactly one character")
        if sample_size_bytes < 1:
            raise ValueError("sample_size_bytes must be greater than zero")
        self.filename = str(filename) if filename is not None else None
        self.format_override = _normalize_format(format_override)
        self.encoding_override = encoding_override
        self.delimiter = delimiter
        self.has_header = has_header
        self.sample_size_bytes = sample_size_bytes

    def parse(self, source: BinaryIO) -> Iterator[DelimitedRecord]:
        """Yield rows from a binary source using detected or overridden settings."""
        sample = source.read(self.sample_size_bytes)
        detection = detect_file(
            sample,
            self.filename,
            sample_size_bytes=self.sample_size_bytes,
        )
        file_format = self.format_override or detection.file_format
        if file_format not in {FileFormat.CSV, FileFormat.TSV}:
            raise DelimitedParseError(
                f"Expected CSV or TSV input, detected {file_format.value!r}; "
                "provide format_override to force delimited parsing"
            )

        encoding = self.encoding_override or detection.encoding
        if encoding is None:
            raise DelimitedParseError("Could not detect a text encoding; provide encoding_override")
        try:
            reader, text_stream = _create_reader(
                source,
                sample,
                encoding=encoding,
                delimiter=self.delimiter or _delimiter_for(file_format),
            )
        except (LookupError, UnicodeError) as exc:
            raise DelimitedParseError(f"Unsupported or invalid text encoding {encoding!r}") from exc

        try:
            yield from self._iter_records(reader)
        except UnicodeDecodeError as exc:
            raise DelimitedParseError(
                f"Could not decode {self.filename or 'delimited source'} using {encoding!r}: {exc}"
            ) from exc
        except csv.Error as exc:
            raise DelimitedParseError(
                f"Malformed delimited input in {self.filename or 'source'} "
                f"near physical line {reader.line_num}: {exc}"
            ) from exc
        finally:
            _detach_text_stream(text_stream)

    def _iter_records(self, reader: csv.reader) -> Iterator[DelimitedRecord]:
        try:
            first_row = next(reader)
        except StopIteration:
            return

        row_index = 0
        if self.has_header:
            headers = _normalize_headers(first_row)
        else:
            row_index = 1
            headers = _column_names(len(first_row))
            yield _record_from_row(
                index=row_index,
                row=first_row,
                headers=headers,
                expected_columns=len(headers),
                row_number=reader.line_num,
            )

        expected_columns = len(headers)
        for row in reader:
            row_index += 1
            yield _record_from_row(
                index=row_index,
                row=row,
                headers=headers,
                expected_columns=expected_columns,
                row_number=reader.line_num,
            )


class CSVParser(DelimitedTextParser):
    """Parse CSV with an optional delimiter or encoding override."""

    def __init__(
        self,
        *,
        filename: str | Path | None = None,
        encoding_override: str | None = None,
        delimiter: str | None = None,
        has_header: bool = True,
        sample_size_bytes: int = DEFAULT_SAMPLE_SIZE_BYTES,
    ) -> None:
        super().__init__(
            filename=filename,
            format_override=FileFormat.CSV,
            encoding_override=encoding_override,
            delimiter=delimiter,
            has_header=has_header,
            sample_size_bytes=sample_size_bytes,
        )


class TSVParser(DelimitedTextParser):
    """Parse TSV with an optional encoding override."""

    def __init__(
        self,
        *,
        filename: str | Path | None = None,
        encoding_override: str | None = None,
        has_header: bool = True,
        sample_size_bytes: int = DEFAULT_SAMPLE_SIZE_BYTES,
    ) -> None:
        super().__init__(
            filename=filename,
            format_override=FileFormat.TSV,
            encoding_override=encoding_override,
            has_header=has_header,
            sample_size_bytes=sample_size_bytes,
        )


class _PrefixedReader(io.RawIOBase):
    """Replay the detection sample before continuing from the original stream."""

    def __init__(self, prefix: bytes, source: BinaryIO) -> None:
        super().__init__()
        self._prefix = memoryview(prefix)
        self._offset = 0
        self._source = source

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: bytearray | memoryview) -> int:
        if self.closed:
            raise ValueError("I/O operation on closed parser stream")
        view = memoryview(buffer)
        written = 0
        if self._offset < len(self._prefix):
            written = min(len(view), len(self._prefix) - self._offset)
            view[:written] = self._prefix[self._offset : self._offset + written]
            self._offset += written
        if written < len(view):
            chunk = self._source.read(len(view) - written)
            if not isinstance(chunk, bytes):
                raise TypeError("parser source streams must return bytes")
            view[written : written + len(chunk)] = chunk
            written += len(chunk)
        return written


def _create_reader(
    source: BinaryIO,
    sample: bytes,
    *,
    encoding: str,
    delimiter: str,
) -> tuple[csv.reader, io.TextIOWrapper]:
    raw_reader = _PrefixedReader(sample, source)
    buffered = io.BufferedReader(raw_reader)
    text_stream = io.TextIOWrapper(buffered, encoding=encoding, newline="")
    return csv.reader(text_stream, delimiter=delimiter, strict=True), text_stream


def _detach_text_stream(text_stream: io.TextIOWrapper) -> None:
    try:
        buffered = text_stream.detach()
    except (AttributeError, ValueError):
        return
    try:
        raw_stream = buffered.detach()
    except (AttributeError, ValueError):
        return
    raw_stream.close()


def _normalize_format(value: FileFormat | str | None) -> FileFormat | None:
    if value is None:
        return None
    try:
        normalized = value if isinstance(value, FileFormat) else FileFormat(value.lower())
    except (AttributeError, ValueError) as exc:
        raise ValueError(f"Unsupported format override: {value!r}") from exc
    if normalized not in {FileFormat.CSV, FileFormat.TSV}:
        raise ValueError("format_override must be 'csv' or 'tsv'")
    return normalized


def _delimiter_for(file_format: FileFormat) -> str:
    return "\t" if file_format == FileFormat.TSV else ","


def _normalize_headers(row: list[str]) -> tuple[str, ...]:
    headers: list[str] = []
    seen: set[str] = set()
    for index, value in enumerate(row, start=1):
        name = value if value else f"column_{index}"
        candidate = name
        suffix = 2
        while candidate in seen:
            candidate = f"{name}__{suffix}"
            suffix += 1
        seen.add(candidate)
        headers.append(candidate)
    return tuple(headers)


def _column_names(count: int) -> tuple[str, ...]:
    return tuple(f"column_{index}" for index in range(1, count + 1))


def _record_from_row(
    *,
    index: int,
    row: list[str],
    headers: tuple[str, ...],
    expected_columns: int,
    row_number: int,
) -> DelimitedRecord:
    values = {headers[position]: value for position, value in enumerate(row[:expected_columns])}
    extra_values = tuple(row[expected_columns:])
    warnings: list[ParseWarning] = []
    if len(row) < expected_columns:
        warnings.append(
            ParseWarning(
                code="too_few_columns",
                message=f"Expected {expected_columns} columns but found {len(row)}",
                row_number=row_number,
            )
        )
    elif len(row) > expected_columns:
        warnings.append(
            ParseWarning(
                code="too_many_columns",
                message=f"Expected {expected_columns} columns but found {len(row)}",
                row_number=row_number,
            )
        )
    return DelimitedRecord(
        index=index,
        values=values,
        extra_values=extra_values,
        warnings=tuple(warnings),
    )
