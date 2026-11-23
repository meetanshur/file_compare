"""Bounded-sample file format, text encoding, and line-ending detection."""

from __future__ import annotations

import codecs
import csv
import json
import re
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from charset_normalizer import from_bytes

DEFAULT_SAMPLE_SIZE_BYTES = 64 * 1024


class FileFormat(StrEnum):
    """Recognized input formats or fallback categories."""

    XML = "xml"
    JSON = "json"
    CSV = "csv"
    TSV = "tsv"
    XLSX = "xlsx"
    PDF = "pdf"
    YAML = "yaml"
    TEXT = "text"
    BINARY = "binary"
    UNKNOWN = "unknown"


class DetectionSource(StrEnum):
    """Evidence source for a detection result."""

    CONTENT = "content"
    EXTENSION = "extension"
    FALLBACK = "fallback"


class EncodingSource(StrEnum):
    """Evidence source for a text encoding result."""

    BOM = "bom"
    UTF8 = "utf-8-validation"
    CHARSET_NORMALIZER = "charset-normalizer"
    BINARY = "binary-content"
    EMPTY = "empty-sample"


class LineEnding(StrEnum):
    """Line-ending style observed in the sampled bytes."""

    CRLF = "CRLF"
    LF = "LF"
    CR = "CR"
    MIXED = "mixed"
    NONE = "none"


@dataclass(frozen=True)
class LineEndingCounts:
    """Counts of line-ending sequences found in the inspected sample."""

    crlf: int = 0
    lf: int = 0
    cr: int = 0


@dataclass(frozen=True)
class FileDetection:
    """Format, encoding, and newline observations for one bounded sample."""

    file_format: FileFormat
    format_source: DetectionSource
    encoding: str | None
    encoding_source: EncodingSource
    is_binary: bool
    line_ending: LineEnding
    line_ending_counts: LineEndingCounts
    sample_size_bytes: int


_EXTENSION_FORMATS = {
    ".xml": FileFormat.XML,
    ".json": FileFormat.JSON,
    ".csv": FileFormat.CSV,
    ".tsv": FileFormat.TSV,
    ".xlsx": FileFormat.XLSX,
    ".pdf": FileFormat.PDF,
    ".yaml": FileFormat.YAML,
    ".yml": FileFormat.YAML,
    ".txt": FileFormat.TEXT,
    ".text": FileFormat.TEXT,
    ".log": FileFormat.TEXT,
}

_BOMS = (
    (codecs.BOM_UTF32_LE, "utf-32"),
    (codecs.BOM_UTF32_BE, "utf-32"),
    (codecs.BOM_UTF8, "utf-8-sig"),
    (codecs.BOM_UTF16_LE, "utf-16"),
    (codecs.BOM_UTF16_BE, "utf-16"),
)
_XML_PREFIX = re.compile(r"^(?:<\?xml\b[^>]*>\s*)?<", re.IGNORECASE)


def detect_file(
    sample: bytes,
    filename: str | Path | None = None,
    *,
    sample_size_bytes: int = DEFAULT_SAMPLE_SIZE_BYTES,
) -> FileDetection:
    """Detect likely format, encoding, and line endings from a bounded sample.

    ``sample`` should come from the start of the source and be limited by the
    caller (the architecture default is 64 KiB). Content evidence takes
    precedence over the filename extension. Newline counts describe only the
    inspected sample, not the complete file.
    """
    if not isinstance(sample, bytes):
        raise TypeError("sample must be bytes")
    if sample_size_bytes < 1:
        raise ValueError("sample_size_bytes must be greater than zero")
    sample = sample[:sample_size_bytes]

    encoding, encoding_source, is_binary, text = _detect_encoding(sample)
    line_ending_counts = _count_line_endings(sample, encoding, text)
    extension_format = _format_from_extension(filename)
    content_format = _format_from_content(sample, text, extension_format)
    if content_format in {FileFormat.PDF, FileFormat.XLSX}:
        encoding = None
        encoding_source = EncodingSource.BINARY
        is_binary = True

    if content_format is not None:
        file_format = content_format
        format_source = DetectionSource.CONTENT
    elif extension_format is not None:
        file_format = extension_format
        format_source = DetectionSource.EXTENSION
    elif is_binary:
        file_format = FileFormat.BINARY
        format_source = DetectionSource.FALLBACK
    elif sample:
        file_format = FileFormat.TEXT
        format_source = DetectionSource.FALLBACK
    else:
        file_format = FileFormat.UNKNOWN
        format_source = DetectionSource.FALLBACK

    return FileDetection(
        file_format=file_format,
        format_source=format_source,
        encoding=encoding,
        encoding_source=encoding_source,
        is_binary=is_binary,
        line_ending=_line_ending_style(line_ending_counts),
        line_ending_counts=line_ending_counts,
        sample_size_bytes=len(sample),
    )


def _detect_encoding(sample: bytes) -> tuple[str | None, EncodingSource, bool, str | None]:
    if not sample:
        return "utf-8", EncodingSource.EMPTY, False, ""

    for bom, name in _BOMS:
        if sample.startswith(bom):
            text = _decode_sample(sample, name)
            return name, EncodingSource.BOM, False, text

    decoder = codecs.getincrementaldecoder("utf-8")(errors="strict")
    try:
        text = decoder.decode(sample, final=False)
    except UnicodeDecodeError:
        text = None
    if text is not None and not _has_binary_controls(text):
        return "utf-8", EncodingSource.UTF8, False, text

    matches = from_bytes(sample)
    match = max(
        matches,
        key=lambda candidate: (candidate.coherence, -candidate.chaos),
        default=None,
    )
    if match is not None:
        try:
            encoding = codecs.lookup(match.encoding).name
            text = _decode_sample(sample, encoding)
        except (LookupError, UnicodeDecodeError):
            pass
        else:
            if not _has_binary_controls(text):
                return encoding, EncodingSource.CHARSET_NORMALIZER, False, text

    return None, EncodingSource.BINARY, True, None


def _decode_sample(sample: bytes, encoding: str) -> str:
    decoder = codecs.getincrementaldecoder(encoding)(errors="replace")
    return decoder.decode(sample, final=False)


def _has_binary_controls(text: str) -> bool:
    if not text:
        return False
    controls = sum(ord(character) < 32 and character not in "\t\n\r\f\b" for character in text)
    return controls / len(text) > 0.01


def _format_from_extension(filename: str | Path | None) -> FileFormat | None:
    if filename is None:
        return None
    return _EXTENSION_FORMATS.get(Path(filename).suffix.lower())


def _format_from_content(
    sample: bytes,
    text: str | None,
    extension_format: FileFormat | None,
) -> FileFormat | None:
    if sample.startswith(b"%PDF-"):
        return FileFormat.PDF
    if sample.startswith(b"PK\x03\x04") and extension_format == FileFormat.XLSX:
        return FileFormat.XLSX
    if text is None:
        return None

    stripped = text.lstrip("\ufeff \t\r\n")
    if not stripped:
        return None
    if _XML_PREFIX.match(stripped):
        return FileFormat.XML
    if stripped.startswith(("{", "[")) and _looks_like_json(stripped):
        return FileFormat.JSON
    if stripped.startswith(("---\n", "---\r", "%YAML ")):
        return FileFormat.YAML

    delimiter = _detect_delimiter(stripped)
    if delimiter == "\t":
        return FileFormat.TSV
    if delimiter == ",":
        return FileFormat.CSV
    return None


def _looks_like_json(text: str) -> bool:
    try:
        _, end = json.JSONDecoder().raw_decode(text)
    except json.JSONDecodeError:
        return False
    return not text[end:].strip()


def _detect_delimiter(text: str) -> str | None:
    sample = text[:DEFAULT_SAMPLE_SIZE_BYTES]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",\t")
    except csv.Error:
        return None
    if dialect.delimiter not in {",", "\t"}:
        return None
    return dialect.delimiter


def _count_line_endings(
    sample: bytes,
    encoding: str | None,
    text: str | None,
) -> LineEndingCounts:
    if text is not None and encoding not in {None, "utf-8", "utf-8-sig"}:
        return _count_text_line_endings(text)
    if encoding in {"utf-8", "utf-8-sig"} and text is not None:
        return _count_text_line_endings(text)

    crlf = sample.count(b"\r\n")
    without_crlf = sample.replace(b"\r\n", b"")
    return LineEndingCounts(
        crlf=crlf,
        lf=without_crlf.count(b"\n"),
        cr=without_crlf.count(b"\r"),
    )


def _count_text_line_endings(text: str) -> LineEndingCounts:
    crlf = text.count("\r\n")
    without_crlf = text.replace("\r\n", "")
    return LineEndingCounts(
        crlf=crlf,
        lf=without_crlf.count("\n"),
        cr=without_crlf.count("\r"),
    )


def _line_ending_style(counts: LineEndingCounts) -> LineEnding:
    active_styles = sum(count > 0 for count in (counts.crlf, counts.lf, counts.cr))
    if active_styles == 0:
        return LineEnding.NONE
    if active_styles > 1:
        return LineEnding.MIXED
    if counts.crlf:
        return LineEnding.CRLF
    if counts.lf:
        return LineEnding.LF
    return LineEnding.CR
