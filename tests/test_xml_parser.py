from __future__ import annotations

from io import BytesIO

import pytest

from parsers.xml import XMLParseError, iter_xml_records


def test_xml_parser_streams_direct_root_children_and_preserves_structure() -> None:
    source = BytesIO(
        b'<records source="test"><record id="007">  <name>Ada</name> tail </record>'
        b'<record id="008"><name>Lin</name></record></records>'
    )

    records = list(iter_xml_records(source, source_name="records.xml"))

    assert [record.index for record in records] == [1, 2]
    first = records[0].element
    assert first.tag == "record"
    assert first.attributes == (("id", "007"),)
    assert first.text == "  "
    assert first.children[0].tag == "name"
    assert first.children[0].text == "Ada"
    assert first.children[0].tail == " tail "
    assert records[1].element.attributes == (("id", "008"),)


def test_xml_parser_selects_namespaced_records_by_local_name() -> None:
    source = BytesIO(
        b'<root xmlns="urn:example"><item><value>one</value></item>'
        b"<item><value>two</value></item></root>"
    )

    records = list(iter_xml_records(source, record_tag="item"))

    assert len(records) == 2
    assert records[0].element.tag == "{urn:example}item"
    assert records[1].element.children[0].text == "two"


def test_xml_parser_selects_by_exact_expanded_qname() -> None:
    source = BytesIO(b'<root xmlns="urn:example"><item><value>one</value></item></root>')

    records = list(iter_xml_records(source, record_tag="{urn:example}item"))

    assert len(records) == 1
    assert records[0].element.tag == "{urn:example}item"


def test_xml_parser_emits_root_for_single_element_document() -> None:
    records = list(iter_xml_records(BytesIO(b"<root value='x'/>")))

    assert len(records) == 1
    assert records[0].element.tag == "root"
    assert records[0].element.attributes == (("value", "x"),)


def test_xml_parser_yields_records_incrementally_without_reading_whole_source() -> None:
    class TrackedStream(BytesIO):
        max_read_size = 0

        def read(self, size: int = -1) -> bytes:
            self.max_read_size = max(self.max_read_size, size)
            return super().read(size)

    source = TrackedStream(
        b"<root>" + b"".join(b"<record><value>x</value></record>" for _ in range(1000)) + b"</root>"
    )
    records = iter_xml_records(source)

    first = next(records)

    assert first.index == 1
    assert first.element.children[0].text == "x"
    assert source.max_read_size > 0
    assert source.max_read_size < source.getbuffer().nbytes
    assert sum(1 for _ in records) == 999


def test_xml_parser_rejects_nested_record_selector_matches() -> None:
    source = BytesIO(b"<root><record><record/></record></root>")
    records = iter_xml_records(source, record_tag="record")

    with pytest.raises(XMLParseError, match="matches nested elements"):
        next(records)


def test_xml_parser_reports_malformed_input_with_name_and_line() -> None:
    source = BytesIO(b"<root>\n<record></root>")

    with pytest.raises(XMLParseError, match=r"broken.xml at line 2"):
        list(iter_xml_records(source, source_name="broken.xml"))


def test_xml_parser_does_not_resolve_external_entities() -> None:
    payload = (
        b'<!DOCTYPE root [<!ENTITY xxe SYSTEM "file:///sensitive-file">]>'
        b"<root><record>&xxe;</record></root>"
    )

    records = list(iter_xml_records(BytesIO(payload)))

    assert len(records) == 1
    assert records[0].element.children == ()
