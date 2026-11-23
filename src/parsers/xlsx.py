"""Read-only, row-streaming XLSX parsing."""

from __future__ import annotations

import tempfile
import zipfile
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO, Literal
from xml.etree.ElementTree import ParseError

from openpyxl import load_workbook
from openpyxl.utils.exceptions import InvalidFileException

from parsers.base import Parser
from parsers.delimited import ParseWarning
from parsers.detection import DEFAULT_SAMPLE_SIZE_BYTES, FileFormat, detect_file

_COPY_CHUNK_SIZE = 1024 * 1024


class XLSXParseError(ValueError):
    """An XLSX workbook is invalid or could not be streamed."""


@dataclass(frozen=True)
class XLSXCellValue:
    """A cell value together with its interpreted type and Excel format."""

    value: Any
    data_type: Literal["string", "number", "date", "boolean", "blank", "formula"]
    number_format: str
    formula: str | None = None
    cached_value: Any = None


@dataclass(frozen=True)
class XLSXRecord:
    """One worksheet row represented by header-to-cell mappings."""

    index: int
    sheet_name: str
    row_number: int
    values: Mapping[str, Any]
    extra_values: tuple[Any, ...] = ()
    warnings: tuple[ParseWarning, ...] = ()


class XLSXParser(Parser[XLSXRecord]):
    """Parse XLSX rows in read-only mode while spooling input to disk."""

    def __init__(
        self,
        *,
        filename: str | Path | None = None,
        sheet_name: str | None = None,
        has_header: bool = True,
        include_empty_rows: bool = False,
        format_override: FileFormat | str | None = None,
        temp_dir: str | Path | None = None,
        sample_size_bytes: int = DEFAULT_SAMPLE_SIZE_BYTES,
    ) -> None:
        if sample_size_bytes < 4:
            raise ValueError("sample_size_bytes must be at least 4")
        if format_override is not None:
            try:
                normalized = (
                    format_override
                    if isinstance(format_override, FileFormat)
                    else FileFormat(format_override.lower())
                )
            except (AttributeError, ValueError) as exc:
                raise ValueError(f"Unsupported format override: {format_override!r}") from exc
            if normalized != FileFormat.XLSX:
                raise ValueError("format_override must be 'xlsx'")
        self.filename = str(filename) if filename is not None else None
        self.sheet_name = sheet_name
        self.has_header = has_header
        self.include_empty_rows = include_empty_rows
        self.format_override = format_override
        self.temp_dir = Path(temp_dir) if temp_dir is not None else None
        self.sample_size_bytes = sample_size_bytes

    def parse(self, source: BinaryIO) -> Iterator[XLSXRecord]:
        """Yield worksheet records without loading the complete workbook in memory."""
        try:
            with tempfile.TemporaryFile(mode="w+b", dir=self.temp_dir) as workbook_file:
                sample = _spool_source(source, workbook_file, self.sample_size_bytes)
                detected = detect_file(
                    sample,
                    self.filename,
                    sample_size_bytes=self.sample_size_bytes,
                )
                if self.format_override is None and detected.file_format != FileFormat.XLSX:
                    raise XLSXParseError(
                        f"Expected XLSX input, detected {detected.file_format.value!r}; "
                        "provide format_override='xlsx' to force parsing"
                    )

                workbook_file.seek(0)
                workbook = load_workbook(
                    workbook_file,
                    read_only=True,
                    data_only=False,
                )
                cached_file = tempfile.TemporaryFile(mode="w+b", dir=self.temp_dir)
                try:
                    _copy_file(workbook_file, cached_file)
                    cached_file.seek(0)
                    cached_workbook = load_workbook(
                        cached_file,
                        read_only=True,
                        data_only=True,
                    )
                    try:
                        yield from self._iter_workbook(workbook, cached_workbook)
                    finally:
                        cached_workbook.close()
                finally:
                    cached_file.close()
                    workbook.close()
        except XLSXParseError:
            raise
        except (
            OSError,
            InvalidFileException,
            ValueError,
            KeyError,
            zipfile.BadZipFile,
            ParseError,
        ) as exc:
            raise XLSXParseError(
                f"Could not parse XLSX source {self.filename or '<stream>'} ({type(exc).__name__})"
            ) from exc

    def _iter_workbook(self, workbook, cached_workbook) -> Iterator[XLSXRecord]:
        if self.sheet_name is not None:
            if self.sheet_name not in workbook.sheetnames:
                raise XLSXParseError(f"Worksheet {self.sheet_name!r} does not exist")
            worksheets = ((workbook[self.sheet_name], cached_workbook[self.sheet_name]),)
        else:
            worksheets = zip(workbook.worksheets, cached_workbook.worksheets, strict=True)

        for worksheet, cached_worksheet in worksheets:
            yield from self._iter_worksheet(worksheet, cached_worksheet)

    def _iter_worksheet(self, worksheet, cached_worksheet) -> Iterator[XLSXRecord]:
        rows = worksheet.iter_rows(values_only=False)
        cached_rows = cached_worksheet.iter_rows(values_only=False)
        try:
            first_row = next(rows)
            cached_first_row = next(cached_rows)
        except StopIteration:
            return

        if self.has_header:
            headers = _normalize_headers(tuple(cell.value for cell in first_row))
            data_rows = zip(rows, cached_rows, strict=True)
            first_row_number = 2
        else:
            headers = _column_names(len(first_row))
            data_rows = zip(
                _prepend(first_row, rows),
                _prepend(cached_first_row, cached_rows),
                strict=True,
            )
            first_row_number = 1

        index = 0
        for row_number, (row, cached_row) in enumerate(data_rows, start=first_row_number):
            if not self.include_empty_rows and all(cell.value is None for cell in row):
                continue
            index += 1
            values = tuple(
                _cell_value(cell, cached_cell)
                for cell, cached_cell in zip(row, cached_row, strict=True)
            )
            yield _record_from_row(index, worksheet.title, row_number, values, headers)


def _spool_source(source: BinaryIO, destination, sample_size_bytes: int) -> bytes:
    sample = bytearray()
    while True:
        chunk = source.read(_COPY_CHUNK_SIZE)
        if not isinstance(chunk, bytes):
            raise TypeError("parser source streams must return bytes")
        if not chunk:
            break
        destination.write(chunk)
        if len(sample) < sample_size_bytes:
            sample.extend(chunk[: sample_size_bytes - len(sample)])
    if not sample:
        raise XLSXParseError("XLSX source is empty")
    return bytes(sample)


def _copy_file(source, destination) -> None:
    source.seek(0)
    while chunk := source.read(_COPY_CHUNK_SIZE):
        destination.write(chunk)


def _prepend(first, rows):
    yield first
    yield from rows


def _cell_value(cell, cached_cell) -> XLSXCellValue:
    value = cell.value
    cell_type = cell.data_type
    if cell_type == "f":
        data_type: Literal["string", "number", "date", "boolean", "blank", "formula"] = "formula"
        formula = value
        cached_value = cached_cell.value
    elif value is None:
        data_type = "blank"
        formula = None
        cached_value = None
    elif cell_type == "b":
        data_type = "boolean"
        formula = None
        cached_value = None
    elif cell_type == "d":
        data_type = "date"
        formula = None
        cached_value = None
    elif cell_type == "n":
        data_type = "number"
        formula = None
        cached_value = None
    else:
        data_type = "string"
        formula = None
        cached_value = None
    return XLSXCellValue(
        value=value,
        data_type=data_type,
        number_format=getattr(cell, "number_format", None) or "General",
        formula=formula,
        cached_value=cached_value,
    )


def _normalize_headers(row: tuple[Any, ...]) -> tuple[str, ...]:
    headers: list[str] = []
    seen: set[str] = set()
    for index, value in enumerate(row, start=1):
        name = str(value) if value is not None and str(value) else f"column_{index}"
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
    index: int,
    sheet_name: str,
    row_number: int,
    row: tuple[Any, ...],
    headers: tuple[str, ...],
) -> XLSXRecord:
    width = len(headers)
    values = {headers[position]: value for position, value in enumerate(row[:width])}
    extra_values = tuple(row[width:])
    warnings: list[ParseWarning] = []
    if len(row) < width:
        warnings.append(
            ParseWarning(
                code="too_few_columns",
                message=f"Expected {width} columns but found {len(row)}",
                row_number=row_number,
            )
        )
    elif len(row) > width:
        warnings.append(
            ParseWarning(
                code="too_many_columns",
                message=f"Expected {width} columns but found {len(row)}",
                row_number=row_number,
            )
        )
    return XLSXRecord(
        index=index,
        sheet_name=sheet_name,
        row_number=row_number,
        values=values,
        extra_values=extra_values,
        warnings=tuple(warnings),
    )
