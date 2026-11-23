"""Page-by-page text extraction from PDF files."""

from __future__ import annotations

import tempfile
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

from pypdf import PdfReader
from pypdf.errors import PdfReadError

from parsers.base import Parser
from parsers.detection import DEFAULT_SAMPLE_SIZE_BYTES, FileFormat, detect_file

_COPY_CHUNK_SIZE = 1024 * 1024


class PDFParseError(ValueError):
    """A PDF is invalid or does not contain an extractable text layer."""


@dataclass(frozen=True)
class PDFTextLine:
    """One extracted text line with its source page and within-page position."""

    index: int
    page_number: int
    line_number: int
    text: str


class PDFTextParser(Parser[PDFTextLine]):
    """Extract text from one PDF page at a time without performing OCR."""

    def __init__(
        self,
        *,
        filename: str | Path | None = None,
        format_override: FileFormat | str | None = None,
        temp_dir: str | Path | None = None,
        sample_size_bytes: int = DEFAULT_SAMPLE_SIZE_BYTES,
    ) -> None:
        if sample_size_bytes < 5:
            raise ValueError("sample_size_bytes must be at least 5")
        if format_override is not None:
            try:
                normalized = (
                    format_override
                    if isinstance(format_override, FileFormat)
                    else FileFormat(format_override.lower())
                )
            except (AttributeError, ValueError) as exc:
                raise ValueError(f"Unsupported format override: {format_override!r}") from exc
            if normalized != FileFormat.PDF:
                raise ValueError("format_override must be 'pdf'")
        self.filename = str(filename) if filename is not None else None
        self.format_override = format_override
        self.temp_dir = Path(temp_dir) if temp_dir is not None else None
        self.sample_size_bytes = sample_size_bytes

    def parse(self, source: BinaryIO) -> Iterator[PDFTextLine]:
        """Yield extracted lines in document order, retaining their page numbers."""
        try:
            with tempfile.TemporaryFile(mode="w+b", dir=self.temp_dir) as pdf_file:
                sample = _spool_source(source, pdf_file, self.sample_size_bytes)
                detected = detect_file(
                    sample,
                    self.filename,
                    sample_size_bytes=self.sample_size_bytes,
                )
                if self.format_override is None and detected.file_format != FileFormat.PDF:
                    raise PDFParseError(
                        f"Expected PDF input, detected {detected.file_format.value!r}; "
                        "provide format_override='pdf' to force parsing"
                    )

                pdf_file.seek(0)
                reader = PdfReader(pdf_file, strict=False)
                try:
                    yield from _iter_text_lines(reader, self.filename)
                finally:
                    reader.close()
        except PDFParseError:
            raise
        except (OSError, PdfReadError, ValueError, KeyError, EOFError, UnicodeDecodeError) as exc:
            raise PDFParseError(
                f"Could not parse PDF source {self.filename or '<stream>'} ({type(exc).__name__})"
            ) from exc


def _iter_text_lines(reader: PdfReader, filename: str | None) -> Iterator[PDFTextLine]:
    index = 0
    has_text = False
    for page_number, page in enumerate(reader.pages, start=1):
        page_text = page.extract_text()
        if not page_text or not page_text.strip():
            continue
        has_text = True
        for line_number, line in enumerate(page_text.splitlines(), start=1):
            index += 1
            yield PDFTextLine(
                index=index,
                page_number=page_number,
                line_number=line_number,
                text=line,
            )
    if not has_text:
        raise PDFParseError(f"PDF has no text layer or extractable text: {filename or '<stream>'}")


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
        raise PDFParseError("PDF source is empty")
    return bytes(sample)
