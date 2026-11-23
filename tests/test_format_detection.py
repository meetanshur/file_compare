from __future__ import annotations

import codecs

import pytest

from parsers.detection import (
    DetectionSource,
    EncodingSource,
    FileFormat,
    LineEnding,
    detect_file,
)


@pytest.mark.parametrize(
    ("sample", "filename", "expected_format"),
    [
        (b"<?xml version='1.0'?><root/>", "input.txt", FileFormat.XML),
        (b'{"records": []}', "input.xml", FileFormat.JSON),
        (b"id,name\n1,Alice\n2,Bob\n", "input.bin", FileFormat.CSV),
        (b"id\tname\n1\tAlice\n2\tBob\n", "input.csv", FileFormat.TSV),
        (b"%PDF-1.7\n", "input.txt", FileFormat.PDF),
        (b"PK\x03\x04binary", "input.xlsx", FileFormat.XLSX),
        (b"---\nid: 1\n", "input.unknown", FileFormat.YAML),
        (b"ordinary text\n", "input.txt", FileFormat.TEXT),
    ],
)
def test_detect_file_recognizes_content_and_extensions(
    sample: bytes,
    filename: str,
    expected_format: FileFormat,
) -> None:
    result = detect_file(sample, filename)

    assert result.file_format == expected_format
    assert result.sample_size_bytes == len(sample)


def test_content_detection_takes_precedence_over_filename_extension() -> None:
    result = detect_file(b'{"id": 1}', "records.csv")

    assert result.file_format == FileFormat.JSON
    assert result.format_source == DetectionSource.CONTENT


def test_extension_is_reported_as_the_format_evidence_when_used() -> None:
    result = detect_file(b"not enough structure to sniff\n", "report.yaml")

    assert result.file_format == FileFormat.YAML
    assert result.format_source == DetectionSource.EXTENSION


@pytest.mark.parametrize(
    ("sample", "expected_encoding"),
    [
        (codecs.BOM_UTF8 + "name\ncaf\u00e9\n".encode(), "utf-8-sig"),
        (codecs.BOM_UTF16_LE + "name\r\nvalue\r\n".encode("utf-16-le"), "utf-16"),
        (codecs.BOM_UTF32_BE + "name\nvalue\n".encode("utf-32-be"), "utf-32"),
        (b"plain ASCII text\n", "utf-8"),
    ],
)
def test_detect_file_identifies_boms_and_utf8(
    sample: bytes,
    expected_encoding: str,
) -> None:
    result = detect_file(sample)

    assert result.encoding == expected_encoding
    assert not result.is_binary


def test_detect_file_uses_charset_normalizer_for_legacy_text() -> None:
    text = "name,description\n" + (
        "caf\u00e9 price \u20ac quote \u201chello\u201d \u2013 \u00a310\n" * 100
    )
    sample = text.encode("cp1252")

    result = detect_file(sample)

    assert result.encoding == "cp1252"
    assert result.encoding_source == EncodingSource.CHARSET_NORMALIZER
    assert not result.is_binary


@pytest.mark.parametrize(
    ("sample", "expected_style", "counts"),
    [
        (b"a\r\nb\r\n", LineEnding.CRLF, (2, 0, 0)),
        (b"a\nb\n", LineEnding.LF, (0, 2, 0)),
        (b"a\rb\r", LineEnding.CR, (0, 0, 2)),
        (b"a\r\nb\nc\r", LineEnding.MIXED, (1, 1, 1)),
        (b"single line", LineEnding.NONE, (0, 0, 0)),
    ],
)
def test_detect_file_counts_line_endings(
    sample: bytes,
    expected_style: LineEnding,
    counts: tuple[int, int, int],
) -> None:
    result = detect_file(sample)

    assert result.line_ending == expected_style
    assert (
        result.line_ending_counts.crlf,
        result.line_ending_counts.lf,
        result.line_ending_counts.cr,
    ) == counts


def test_detect_file_counts_utf16_line_endings_after_decoding() -> None:
    sample = codecs.BOM_UTF16_LE + "first\r\nsecond\n".encode("utf-16-le")

    result = detect_file(sample)

    assert result.line_ending == LineEnding.MIXED
    assert result.line_ending_counts.crlf == 1
    assert result.line_ending_counts.lf == 1


def test_detect_file_classifies_unknown_binary_and_empty_samples() -> None:
    binary = detect_file(b"\x00\x01\x02\x03\x00", "unknown.dat")
    empty = detect_file(b"")

    assert binary.file_format == FileFormat.BINARY
    assert binary.is_binary
    assert binary.encoding is None
    assert binary.encoding_source == EncodingSource.BINARY
    assert empty.file_format == FileFormat.UNKNOWN
    assert empty.encoding == "utf-8"
    assert empty.encoding_source == EncodingSource.EMPTY


def test_detect_file_requires_a_bytes_sample() -> None:
    with pytest.raises(TypeError, match="sample must be bytes"):
        detect_file("not bytes")  # type: ignore[arg-type]


def test_detect_file_applies_bounded_sample_size() -> None:
    sample = b"plain text\r\n" + (b"x" * 100)

    result = detect_file(sample, sample_size_bytes=12)

    assert result.sample_size_bytes == 12
    assert result.line_ending_counts.crlf == 1


def test_detect_file_requires_positive_sample_size() -> None:
    with pytest.raises(ValueError, match="sample_size_bytes must be greater than zero"):
        detect_file(b"data", sample_size_bytes=0)


@pytest.mark.parametrize(
    "sample",
    [b"%PDF-1.7\n", b"PK\x03\x04binary"],
)
def test_container_formats_are_reported_as_binary(sample: bytes) -> None:
    filename = "report.pdf" if sample.startswith(b"%PDF") else "workbook.xlsx"

    result = detect_file(sample, filename)

    assert result.is_binary
    assert result.encoding is None
    assert result.encoding_source == EncodingSource.BINARY
