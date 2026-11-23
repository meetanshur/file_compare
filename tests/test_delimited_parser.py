from __future__ import annotations

from io import BytesIO

import pytest

from parsers import (
    CSVParser,
    DelimitedParseError,
    DelimitedTextParser,
    FileFormat,
    Parser,
    TSVParser,
    XMLParser,
)


def test_csv_parser_keeps_field_text_and_quoted_field_values() -> None:
    source = BytesIO(b'id,amount,trailing,quoted\r\n007,1.10,"tail   ","hello, ""world"""\r\n')

    record = next(CSVParser(filename="input.csv").parse(source))

    assert record.values == {
        "id": "007",
        "amount": "1.10",
        "trailing": "tail   ",
        "quoted": 'hello, "world"',
    }
    assert record.warnings == ()


def test_tsv_parser_detects_tsv_content_and_preserves_values() -> None:
    source = BytesIO(b"account\tamount\n007\t1.10\n")

    records = list(TSVParser(filename="data.tsv").parse(source))

    assert records[0].values == {"account": "007", "amount": "1.10"}


def test_delimited_parser_uses_detected_format_when_extension_disagrees() -> None:
    source = BytesIO(b"key\tvalue\n007\t1.10\n")

    records = list(DelimitedTextParser(filename="input.csv").parse(source))

    assert records[0].values == {"key": "007", "value": "1.10"}


def test_format_override_selects_parser_when_extension_and_content_are_ambiguous() -> None:
    source = BytesIO(b"left;right\n1;2\n")
    parser = DelimitedTextParser(
        filename="input.txt",
        format_override=FileFormat.CSV,
        delimiter=";",
    )

    records = list(parser.parse(source))

    assert records[0].values == {"left": "1", "right": "2"}


def test_encoding_override_decodes_text_when_automatic_detection_is_wrong() -> None:
    source = BytesIO("name,value\ncaf\u00e9,1\n".encode("cp1252"))
    parser = CSVParser(filename="legacy.csv", encoding_override="cp1252")

    records = list(parser.parse(source))

    assert records[0].values == {"name": "caf\u00e9", "value": "1"}


def test_detected_bom_encoding_is_removed_from_first_field() -> None:
    source = BytesIO(b"\xef\xbb\xbfid,value\n007,1.10\n")

    records = list(CSVParser(filename="data.csv").parse(source))

    assert records[0].values == {"id": "007", "value": "1.10"}


def test_rows_with_too_few_columns_are_returned_with_warning() -> None:
    source = BytesIO(b"id,name,amount\n1,Ada\n")

    record = list(CSVParser(filename="data.csv").parse(source))[0]

    assert record.values == {"id": "1", "name": "Ada"}
    assert len(record.warnings) == 1
    assert record.warnings[0].code == "too_few_columns"
    assert record.warnings[0].row_number == 2


def test_rows_with_too_many_columns_keep_extra_values_and_warning() -> None:
    source = BytesIO(b"id,name\n1,Ada,unexpected,extra\n")

    record = list(CSVParser(filename="data.csv").parse(source))[0]

    assert record.values == {"id": "1", "name": "Ada"}
    assert record.extra_values == ("unexpected", "extra")
    assert record.warnings[0].code == "too_many_columns"


def test_header_can_be_disabled_and_columns_are_generated() -> None:
    source = BytesIO(b"007,1.10\n008,2.00\n")

    records = list(CSVParser(filename="values.csv", has_header=False).parse(source))

    assert records[0].index == 1
    assert records[0].values == {"column_1": "007", "column_2": "1.10"}
    assert records[1].values == {"column_1": "008", "column_2": "2.00"}


def test_duplicate_and_empty_header_names_are_made_unique() -> None:
    source = BytesIO(b"id,id,\n1,2,3\n")

    record = list(CSVParser(filename="data.csv").parse(source))[0]

    assert record.values == {"id": "1", "id__2": "2", "column_3": "3"}


def test_empty_input_yields_no_records() -> None:
    assert list(CSVParser(filename="empty.csv").parse(BytesIO())) == []


def test_csv_parser_reads_from_stream_in_bounded_blocks() -> None:
    class TrackedStream(BytesIO):
        requests: list[int] = []

        def read(self, size: int = -1) -> bytes:
            self.requests.append(size)
            return super().read(size)

    source = TrackedStream(b"id,value\n" + b"".join(b"1,record\n" for _ in range(20_000)))
    records = CSVParser(filename="large.csv").parse(source)

    assert next(records).values == {"id": "1", "value": "record"}
    assert source.tell() < len(source.getbuffer())
    assert all(0 < size <= 64 * 1024 for size in source.requests)
    records.close()


def test_non_delimited_input_has_clear_error_unless_overridden() -> None:
    parser = DelimitedTextParser(filename="input.xml")

    with pytest.raises(DelimitedParseError, match="Expected CSV or TSV"):
        list(parser.parse(BytesIO(b"<root/>")))


def test_malformed_csv_reports_parse_error() -> None:
    parser = CSVParser(filename="broken.csv")

    with pytest.raises(DelimitedParseError, match="Malformed delimited input"):
        list(parser.parse(BytesIO(b'a,b\n"unclosed,value\n')))


def test_decode_error_has_clear_encoding_and_source_context() -> None:
    parser = CSVParser(filename="legacy.csv", encoding_override="utf-8")

    with pytest.raises(DelimitedParseError, match="Could not decode legacy.csv"):
        list(parser.parse(BytesIO(b"id,value\n1,\xff\n")))


def test_parser_protocol_is_shared_by_csv_and_xml() -> None:
    assert isinstance(CSVParser(filename="data.csv"), Parser)
    assert isinstance(XMLParser(), Parser)
