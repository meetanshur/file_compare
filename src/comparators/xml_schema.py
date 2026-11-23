"""Streaming XML structure comparison and optional XSD validation."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from itertools import zip_longest
from typing import BinaryIO

from lxml import etree

from common.comparison_config import ComparisonConfig
from common.comparison_result import ComparisonType, Difference, Severity
from common.comparison_rules import ComparisonRules
from parsers.xml import XmlElement, XmlRecord


class XMLSchemaError(ValueError):
    """XML could not be validated against the supplied XML Schema."""


class XMLSchemaComparator:
    """Compare XML record structure without comparing record field values."""

    def compare(
        self,
        records_a: Iterable[XmlRecord],
        records_b: Iterable[XmlRecord],
        *,
        cancellation_check: Callable[[], None] | None = None,
        max_total_differences: int = 0,
        config: ComparisonConfig | None = None,
    ) -> Iterator[Difference]:
        """Yield tag, hierarchy, attribute-name, and element-order differences.

        Record payloads are compared in order, one pair at a time. Attribute
        values and element text are data, not schema, and are not compared.
        """
        if max_total_differences < 0:
            raise ValueError("max_total_differences must be zero or greater")

        missing = object()
        difference_count = 0
        ignore_order = ComparisonRules(config).ignores_element_order if config else False
        for index, (record_a, record_b) in enumerate(
            zip_longest(records_a, records_b, fillvalue=missing),
            start=1,
        ):
            _check_cancelled(cancellation_check)
            if record_a is missing:
                difference = _difference(
                    f"/record[{index}]",
                    "XML record exists only in file B",
                    None,
                    record_b.element.tag,
                )
                yield difference
                difference_count += 1
            elif record_b is missing:
                difference = _difference(
                    f"/record[{index}]",
                    "XML record exists only in file A",
                    record_a.element.tag,
                    None,
                )
                yield difference
                difference_count += 1
            else:
                for difference in _compare_elements(
                    record_a.element,
                    record_b.element,
                    f"/record[{index}]",
                    cancellation_check,
                    ignore_order,
                ):
                    if max_total_differences and difference_count >= max_total_differences:
                        return
                    yield difference
                    difference_count += 1
            if max_total_differences and difference_count >= max_total_differences:
                return


def validate_xml_schema(
    xml_source: BinaryIO,
    xsd_source: BinaryIO,
    *,
    source_name: str = "<stream>",
    schema_name: str = "<schema>",
) -> None:
    """Validate an XML stream incrementally against an XSD.

    External entities and network DTD access are disabled for both schema and
    document parsing. Raises ``XMLSchemaError`` with the source and validator
    message when the document is malformed or fails schema validation.
    """
    try:
        schema_document = etree.parse(xsd_source, _safe_xml_parser())
        schema = etree.XMLSchema(schema_document)
        context = etree.iterparse(
            xml_source,
            events=("end",),
            schema=schema,
            resolve_entities=False,
            no_network=True,
            load_dtd=False,
            huge_tree=False,
        )
        for _, element in context:
            element.clear(keep_tail=True)
            parent = element.getparent()
            if parent is not None:
                while element.getprevious() is not None:
                    del parent[0]
    except (etree.XMLSyntaxError, etree.XMLSchemaError, OSError, ValueError) as exc:
        raise XMLSchemaError(
            f"XML schema validation failed for {source_name} against {schema_name}: {exc}"
        ) from exc


def _safe_xml_parser() -> etree.XMLParser:
    return etree.XMLParser(resolve_entities=False, no_network=True, load_dtd=False, huge_tree=False)


def _compare_elements(
    element_a: XmlElement,
    element_b: XmlElement,
    path: str,
    cancellation_check: Callable[[], None] | None,
    ignore_element_order: bool = False,
) -> Iterator[Difference]:
    stack: list[tuple[XmlElement, XmlElement, str]] = [(element_a, element_b, path)]
    while stack:
        _check_cancelled(cancellation_check)
        current_a, current_b, current_path = stack.pop()
        if current_a.tag != current_b.tag:
            yield _difference(
                current_path,
                "Element name or namespace differs",
                current_a.tag,
                current_b.tag,
            )

        attributes_a = {name for name, _ in current_a.attributes}
        attributes_b = {name for name, _ in current_b.attributes}
        for name in sorted(attributes_a - attributes_b):
            yield _difference(
                f"{current_path}/@{name}",
                "Attribute exists only in file A",
                name,
                None,
            )
        for name in sorted(attributes_b - attributes_a):
            yield _difference(
                f"{current_path}/@{name}",
                "Attribute exists only in file B",
                None,
                name,
            )

        children_a = current_a.children
        children_b = current_b.children
        if ignore_element_order:
            children_a = tuple(sorted(children_a, key=lambda child: child.tag))
            children_b = tuple(sorted(children_b, key=lambda child: child.tag))
        common_count = min(len(children_a), len(children_b))
        for child_index in range(common_count - 1, -1, -1):
            child_a = children_a[child_index]
            child_b = children_b[child_index]
            child_path = (
                f"{current_path}/{child_a.tag}[{child_index + 1}]"
                if ignore_element_order
                else f"{current_path}/element[{child_index + 1}]"
            )
            stack.append((child_a, child_b, child_path))
        for child_index in range(common_count, len(children_a)):
            child = children_a[child_index]
            yield _difference(
                f"{current_path}/element[{child_index + 1}]",
                "Child element exists only in file A",
                child.tag,
                None,
            )
        for child_index in range(common_count, len(children_b)):
            child = children_b[child_index]
            yield _difference(
                f"{current_path}/element[{child_index + 1}]",
                "Child element exists only in file B",
                None,
                child.tag,
            )


def _difference(
    location: str,
    message: str,
    value_a: str | None,
    value_b: str | None,
) -> Difference:
    return Difference(
        comparison_type=ComparisonType.SCHEMA,
        location=location,
        message=message,
        severity=Severity.WARNING,
        value_a=value_a,
        value_b=value_b,
    )


def _check_cancelled(cancellation_check: Callable[[], None] | None) -> None:
    if cancellation_check is not None:
        cancellation_check()
