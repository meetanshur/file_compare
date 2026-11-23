from __future__ import annotations

from io import BytesIO

import pytest

from parsers import (
    FixedWidthParser,
    JSONParser,
    Parser,
    PlainTextParser,
    StructuredParseError,
    YAMLParser,
)


def test_json_parser_yields_top_level_array_values_incrementally() -> None:
    source = BytesIO(b'[{"id":"007","amount":1.10},{"id":"008","amount":2}]')

    records = list(JSONParser(filename="records.json").parse(source))

    assert [record.index for record in records] == [1, 2]
    assert records[0].value == {"id": "007", "amount": 1.10}


def test_json_parser_yields_single_object_or_scalar_document() -> None:
    object_records = list(JSONParser(filename="single.json").parse(BytesIO(b'{"x":[1,2]}')))
    scalar_records = list(JSONParser(filename="scalar.json").parse(BytesIO(b"true")))

    assert object_records[0].value == {"x": [1, 2]}
    assert scalar_records[0].value is True


def test_json_lines_mode_yields_each_nonblank_line() -> None:
    source = BytesIO(b'{"id":1}\n\n{"id":2}\r\n')

    records = list(JSONParser(filename="records.jsonl", json_lines=True).parse(source))

    assert [record.value for record in records] == [{"id": 1}, {"id": 2}]
    assert [record.index for record in records] == [1, 2]


def test_json_parser_reports_malformed_array_and_trailing_content() -> None:
    with pytest.raises(StructuredParseError, match="Malformed JSON array"):
        list(JSONParser(filename="bad.json").parse(BytesIO(b'[{"x":]')))
    with pytest.raises(StructuredParseError, match="Unexpected trailing data"):
        list(JSONParser(filename="bad.json").parse(BytesIO(b"[] false")))


def test_json_lines_reports_source_line_for_malformed_record() -> None:
    parser = JSONParser(filename="bad.jsonl", json_lines=True)

    with pytest.raises(StructuredParseError, match="bad.jsonl at line 2"):
        list(parser.parse(BytesIO(b'{"ok":1}\n{"broken":}\n')))


def test_json_parser_is_incremental_for_large_top_level_arrays() -> None:
    class TrackedStream(BytesIO):
        requests: list[int]

        def __init__(self, value: bytes) -> None:
            super().__init__(value)
            self.requests = []

        def read(self, size: int = -1) -> bytes:
            self.requests.append(size)
            return super().read(size)

    source = TrackedStream(b"[" + b",".join(b'{"id":1}' for _ in range(50_000)) + b"]")
    records = JSONParser(filename="large.json").parse(source)

    assert next(records).value == {"id": 1}
    assert source.tell() < len(source.getbuffer())
    assert all(0 < size <= 64 * 1024 for size in source.requests)
    records.close()


def test_yaml_parser_streams_multiple_safe_documents() -> None:
    source = BytesIO(b"---\nid: 1\n---\nid: 2\n")

    records = list(YAMLParser(filename="records.yaml").parse(source))

    assert [record.value for record in records] == [{"id": 1}, {"id": 2}]
    assert [record.index for record in records] == [1, 2]


def test_yaml_parser_uses_safe_loader_and_rejects_python_object_tags() -> None:
    source = BytesIO(b"!!python/object/apply:os.system ['echo unsafe']\n")

    with pytest.raises(StructuredParseError, match="Malformed YAML"):
        list(YAMLParser(filename="unsafe.yaml").parse(source))


def test_yaml_parser_reports_decode_errors() -> None:
    parser = YAMLParser(filename="legacy.yaml", encoding_override="utf-8")

    with pytest.raises(StructuredParseError, match="Could not decode legacy.yaml"):
        list(parser.parse(BytesIO(b"\xff")))


def test_fixed_width_parser_preserves_spaces_and_warns_on_short_rows() -> None:
    source = BytesIO(b"0071.10tail   \n0082.00short\n")
    parser = FixedWidthParser(
        {"id": 3, "amount": 4, "description": 7},
        filename="rows.fwf",
    )

    records = list(parser.parse(source))

    assert records[0].values == {"id": "007", "amount": "1.10", "description": "tail   "}
    assert records[0].warnings == ()
    assert records[1].values == {"id": "008", "amount": "2.00", "description": "short"}
    assert records[1].warnings[0].code == "too_few_characters"


def test_fixed_width_parser_retains_extra_text_and_validates_configuration() -> None:
    record = next(FixedWidthParser((2, 2), filename="rows.txt").parse(BytesIO(b"ABCDextra\n")))

    assert record.values == {"column_1": "AB", "column_2": "CD"}
    assert record.extra_text == "extra"
    assert record.warnings[0].code == "too_many_characters"
    with pytest.raises(ValueError, match="non-empty sequence of positives"):
        FixedWidthParser((2, 0))


def test_plain_text_parser_preserves_whitespace_and_line_terminators() -> None:
    source = BytesIO(b" trailing spaces  \r\nsecond\tline\nlast")

    records = list(PlainTextParser(filename="notes.txt").parse(source))

    assert [record.text for record in records] == [
        " trailing spaces  \r\n",
        "second\tline\n",
        "last",
    ]


def test_text_parsers_accept_encoding_override_for_legacy_text() -> None:
    source = BytesIO("café\r\n".encode("cp1252"))

    records = list(PlainTextParser(filename="notes.txt", encoding_override="cp1252").parse(source))

    assert records[0].text == "café\r\n"


def test_plain_text_parser_yields_records_from_a_stream_not_a_full_read() -> None:
    class TrackedStream(BytesIO):
        requests: list[int]

        def __init__(self, value: bytes) -> None:
            super().__init__(value)
            self.requests = []

        def read(self, size: int = -1) -> bytes:
            self.requests.append(size)
            return super().read(size)

    source = TrackedStream(b"line\n" * 100_000)
    records = PlainTextParser(filename="large.txt").parse(source)

    assert next(records).text == "line\n"
    assert source.tell() < len(source.getbuffer())
    assert all(0 < size <= 64 * 1024 for size in source.requests)
    records.close()


def test_json_yaml_and_text_parsers_share_parser_protocol() -> None:
    assert isinstance(JSONParser(filename="file.json"), Parser)
    assert isinstance(YAMLParser(filename="file.yaml"), Parser)
    assert isinstance(FixedWidthParser((4,), filename="file.fwf"), Parser)
    assert isinstance(PlainTextParser(filename="file.txt"), Parser)
