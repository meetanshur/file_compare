from __future__ import annotations

from io import BytesIO

import pytest
from pypdf import PdfWriter
from pypdf.generic import DecodedStreamObject, DictionaryObject, NameObject

from parsers import Parser, PDFParseError, PDFTextLine, PDFTextParser


def _blank_pdf_bytes() -> bytes:
    writer = PdfWriter()
    writer.add_blank_page(width=72, height=72)
    output = BytesIO()
    writer.write(output)
    return output.getvalue()


def _text_pdf_bytes() -> bytes:
    writer = PdfWriter()
    _add_text_page(writer, b"(alpha) Tj 0 -14 Td (beta) Tj")
    writer.add_blank_page(width=72, height=72)
    _add_text_page(writer, b"(gamma) Tj")
    output = BytesIO()
    writer.write(output)
    return output.getvalue()


def _add_text_page(writer: PdfWriter, text_commands: bytes) -> None:
    page = writer.add_blank_page(width=72, height=72)
    font = DictionaryObject(
        {
            NameObject("/Type"): NameObject("/Font"),
            NameObject("/Subtype"): NameObject("/Type1"),
            NameObject("/BaseFont"): NameObject("/Helvetica"),
        }
    )
    font_reference = writer._add_object(font)
    page[NameObject("/Resources")] = DictionaryObject(
        {
            NameObject("/Font"): DictionaryObject({NameObject("/F1"): font_reference}),
        }
    )
    content = DecodedStreamObject()
    content.set_data(b"BT /F1 12 Tf 10 50 Td " + text_commands + b" ET")
    page[NameObject("/Contents")] = writer._add_object(content)


def test_pdf_parser_yields_text_lines_with_page_and_line_numbers() -> None:
    source = BytesIO(_text_pdf_bytes())

    records = list(PDFTextParser(filename="report.pdf").parse(source))

    assert records == [
        PDFTextLine(index=1, page_number=1, line_number=1, text="alpha"),
        PDFTextLine(index=2, page_number=1, line_number=2, text="beta"),
        PDFTextLine(index=3, page_number=3, line_number=1, text="gamma"),
    ]
    assert source.closed is False


def test_pdf_parser_reports_scanned_pdf_without_text_layer() -> None:
    with pytest.raises(PDFParseError, match="no text layer or extractable text"):
        list(PDFTextParser(filename="scanned.pdf").parse(BytesIO(_blank_pdf_bytes())))


def test_pdf_parser_rejects_non_pdf_without_override() -> None:
    with pytest.raises(PDFParseError, match="Expected PDF input"):
        list(PDFTextParser(filename="data.txt").parse(BytesIO(b"plain text")))


def test_pdf_parser_conforms_to_common_parser_protocol() -> None:
    assert isinstance(PDFTextParser(filename="report.pdf"), Parser)
