from __future__ import annotations

from datetime import datetime
from io import BytesIO
from xml.etree import ElementTree
from zipfile import ZIP_DEFLATED, ZipFile

import pytest
from openpyxl import Workbook

from parsers import Parser, XLSXCellValue, XLSXParseError, XLSXParser


def _workbook_bytes() -> bytes:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Data"
    sheet.append(["id", "amount", "description"])
    sheet.append(["007", 1.1, "tail   "])
    sheet.append(["008", 2.0, "second"])
    output = BytesIO()
    workbook.save(output)
    workbook.close()
    return output.getvalue()


def test_xlsx_parser_streams_rows_and_returns_sheet_metadata() -> None:
    source = BytesIO(_workbook_bytes())

    records = list(XLSXParser(filename="data.xlsx").parse(source))

    assert [record.index for record in records] == [1, 2]
    assert records[0].sheet_name == "Data"
    assert records[0].row_number == 2
    assert records[0].values == {
        "id": XLSXCellValue("007", "string", "General"),
        "amount": XLSXCellValue(1.1, "number", "General"),
        "description": XLSXCellValue("tail   ", "string", "General"),
    }
    assert source.closed is False


def test_xlsx_parser_can_select_a_sheet_or_parse_all_sheets() -> None:
    workbook = Workbook()
    workbook.active.title = "First"
    workbook.active.append(["id"])
    workbook.active.append(["1"])
    second = workbook.create_sheet("Second")
    second.append(["id"])
    second.append(["2"])
    output = BytesIO()
    workbook.save(output)
    workbook.close()
    content = output.getvalue()

    all_records = list(XLSXParser(filename="data.xlsx").parse(BytesIO(content)))
    selected_records = list(
        XLSXParser(filename="data.xlsx", sheet_name="Second").parse(BytesIO(content))
    )

    assert [record.sheet_name for record in all_records] == ["First", "Second"]
    assert selected_records[0].values["id"].value == "2"


def test_xlsx_parser_without_header_generates_columns() -> None:
    workbook = Workbook()
    workbook.active.append(["007", "1.10"])
    output = BytesIO()
    workbook.save(output)

    record = next(
        XLSXParser(filename="data.xlsx", has_header=False).parse(BytesIO(output.getvalue()))
    )

    assert {key: cell.value for key, cell in record.values.items()} == {
        "column_1": "007",
        "column_2": "1.10",
    }


def test_xlsx_parser_reports_missing_sheet_and_allows_explicit_format_override() -> None:
    data = _workbook_bytes()
    with pytest.raises(XLSXParseError, match="Worksheet 'Missing' does not exist"):
        list(XLSXParser(filename="data.xlsx", sheet_name="Missing").parse(BytesIO(data)))

    records = list(XLSXParser(filename="data.bin", format_override="xlsx").parse(BytesIO(data)))
    assert records[0].values["id"].value == "007"


def test_xlsx_parser_rejects_non_xlsx_input_without_override() -> None:
    with pytest.raises(XLSXParseError, match="Expected XLSX input"):
        list(XLSXParser(filename="data.csv").parse(BytesIO(b"a,b\n1,2\n")))


def test_xlsx_parser_reports_invalid_workbook_clearly() -> None:
    with pytest.raises(XLSXParseError, match="Could not parse XLSX source broken.xlsx"):
        list(XLSXParser(filename="broken.xlsx").parse(BytesIO(b"not an xlsx archive")))


def test_xlsx_parser_reads_nonseekable_source_with_bounded_reads(tmp_path) -> None:
    class NonSeekableStream:
        def __init__(self, value: bytes) -> None:
            self._stream = BytesIO(value)
            self.requests: list[int] = []
            self.closed = False

        def read(self, size: int = -1) -> bytes:
            self.requests.append(size)
            return self._stream.read(size)

    content = _workbook_bytes()
    source = NonSeekableStream(content)
    parser = XLSXParser(filename="data.xlsx", temp_dir=tmp_path)

    records = list(parser.parse(source))

    assert len(records) == 2
    assert all(0 < size <= 1024 * 1024 for size in source.requests)
    assert source.closed is False
    assert list(tmp_path.iterdir()) == []


def test_xlsx_parser_conforms_to_common_parser_protocol() -> None:
    assert isinstance(XLSXParser(filename="data.xlsx"), Parser)


def test_xlsx_parser_preserves_cell_types_formats_and_formula_cache() -> None:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Typed"
    sheet.append(["text", "number", "date", "blank", "formula"])
    sheet.append(["007", 1.1, datetime(2024, 1, 5), None, "=B2*2"])
    sheet["B2"].number_format = "0.00"
    sheet["C2"].number_format = "yyyy-mm-dd"
    output = BytesIO()
    workbook.save(output)
    workbook.close()
    data = _add_formula_cache(output.getvalue(), "E2", "2.2")

    record = next(XLSXParser(filename="typed.xlsx").parse(BytesIO(data)))

    assert record.sheet_name == "Typed"
    assert record.values["text"] == XLSXCellValue("007", "string", "General")
    assert record.values["number"] == XLSXCellValue(1.1, "number", "0.00")
    assert record.values["date"] == XLSXCellValue(datetime(2024, 1, 5), "date", "yyyy-mm-dd")
    assert record.values["blank"] == XLSXCellValue(None, "blank", "General")
    assert record.values["formula"] == XLSXCellValue(
        "=B2*2",
        "formula",
        "General",
        formula="=B2*2",
        cached_value=2.2,
    )


def _add_formula_cache(data: bytes, cell_reference: str, cached_value: str) -> bytes:
    input_archive = BytesIO(data)
    output_archive = BytesIO()
    with ZipFile(input_archive) as source, ZipFile(output_archive, "w", ZIP_DEFLATED) as target:
        for item in source.infolist():
            contents = source.read(item.filename)
            if item.filename == "xl/worksheets/sheet1.xml":
                root = ElementTree.fromstring(contents)
                namespace = {"main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
                cell = root.find(f".//main:c[@r='{cell_reference}']", namespace)
                assert cell is not None
                value = cell.find("main:v", namespace)
                assert value is not None
                value.text = cached_value
                contents = ElementTree.tostring(root)
            target.writestr(item, contents)
    return output_archive.getvalue()
