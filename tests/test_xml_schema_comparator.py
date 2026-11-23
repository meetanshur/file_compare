from __future__ import annotations

from io import BytesIO

import pytest

from common.comparison_result import ComparisonType
from comparators import XMLSchemaComparator, XMLSchemaError, validate_xml_schema
from parsers.xml import iter_xml_records


def test_xml_schema_comparator_detects_tag_hierarchy_attribute_and_order_changes() -> None:
    records_a = iter_xml_records(
        BytesIO(
            b'<records xmlns="urn:a"><record id="1" status="ok">'
            b"<name>Ada</name><amount>1.10</amount></record></records>"
        )
    )
    records_b = iter_xml_records(
        BytesIO(
            b'<records xmlns="urn:b"><record id="1" state="ok">'
            b"<amount>1.10</amount><name>Ada</name></record></records>"
        )
    )

    differences = list(XMLSchemaComparator().compare(records_a, records_b))

    assert len(differences) >= 5
    assert all(item.comparison_type is ComparisonType.SCHEMA for item in differences)
    assert any("namespace differs" in item.message for item in differences)
    assert any(item.location.endswith("/@status") for item in differences)
    assert any(item.location.endswith("/@state") for item in differences)
    assert any(item.location.endswith("/element[1]") for item in differences)


def test_xml_schema_comparator_ignores_text_and_attribute_values() -> None:
    records_a = iter_xml_records(
        BytesIO(b'<root><record id="007" flag="A"><value>1.10</value></record></root>')
    )
    records_b = iter_xml_records(
        BytesIO(b'<root><record id="008" flag="B"><value>1.20</value></record></root>')
    )

    assert list(XMLSchemaComparator().compare(records_a, records_b)) == []


def test_xml_schema_comparator_reports_missing_child_and_record() -> None:
    records_a = iter_xml_records(BytesIO(b"<root><record><a/><b/></record></root>"))
    records_b = iter_xml_records(BytesIO(b"<root><record><a/></record><record/></root>"))

    differences = list(XMLSchemaComparator().compare(records_a, records_b))

    assert any("Child element exists only in file A" == item.message for item in differences)
    assert any("XML record exists only in file B" == item.message for item in differences)


def test_xml_schema_comparator_honors_difference_limit_and_cancellation() -> None:
    records_a = iter_xml_records(BytesIO(b"<root><record><a/><b/></record></root>"))
    records_b = iter_xml_records(BytesIO(b"<root><record><x/><y/></record></root>"))

    limited = list(XMLSchemaComparator().compare(records_a, records_b, max_total_differences=1))
    assert len(limited) == 1

    def cancelled() -> None:
        raise RuntimeError("cancelled")

    with pytest.raises(RuntimeError, match="cancelled"):
        next(
            XMLSchemaComparator().compare(
                iter_xml_records(BytesIO(b"<root/>")),
                iter_xml_records(BytesIO(b"<other/>")),
                cancellation_check=cancelled,
            )
        )


def test_validate_xml_schema_accepts_valid_document() -> None:
    xsd = BytesIO(
        b"""<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
          <xs:element name="record">
            <xs:complexType><xs:sequence>
              <xs:element name="id" type="xs:string"/>
            </xs:sequence></xs:complexType>
          </xs:element>
        </xs:schema>"""
    )

    validate_xml_schema(BytesIO(b"<record><id>007</id></record>"), xsd)


def test_validate_xml_schema_reports_invalid_document_with_names() -> None:
    xsd = BytesIO(
        b"""<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
          <xs:element name="record"><xs:complexType><xs:sequence>
            <xs:element name="id" type="xs:string"/>
          </xs:sequence></xs:complexType></xs:element>
        </xs:schema>"""
    )

    with pytest.raises(XMLSchemaError, match="records.xml against record.xsd"):
        validate_xml_schema(
            BytesIO(b"<record><unexpected/></record>"),
            xsd,
            source_name="records.xml",
            schema_name="record.xsd",
        )
