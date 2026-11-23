"""Memory-bounded XML record parsing."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import BinaryIO

from lxml import etree


class XMLParseError(ValueError):
    """An XML source is malformed or its record selector is ambiguous."""


@dataclass(frozen=True)
class XmlElement:
    """An immutable snapshot of one XML element and its ordered contents."""

    tag: str
    attributes: tuple[tuple[str, str], ...]
    text: str | None
    children: tuple[XmlElement, ...]
    tail: str | None


@dataclass(frozen=True)
class XmlRecord:
    """A selected XML element emitted as an independent record."""

    index: int
    element: XmlElement


def iter_xml_records(
    source: BinaryIO,
    *,
    record_tag: str | None = None,
    source_name: str = "<stream>",
) -> Iterator[XmlRecord]:
    """Yield root-level XML records, or elements matching ``record_tag``.

    With no selector, every direct child of the document root is one record.
    A document whose root has no child elements yields the root as its single
    record. ``record_tag`` may be an exact XML name (including Clark notation)
    or an unambiguous local name. Matching record elements may not nest.

    Each record is materialized independently as an immutable tree; after it
    is emitted, its source element and preceding siblings are cleared. Memory
    therefore scales with the largest selected record, not the complete
    document. The caller retains ownership of ``source``.
    """
    stack: list[etree._Element] = []
    selected_stack: list[bool] = []
    root_had_children = False
    index = 0

    try:
        context = etree.iterparse(
            source,
            events=("start", "end"),
            resolve_entities=False,
            no_network=True,
            load_dtd=False,
            huge_tree=False,
        )
        for event, element in context:
            if event == "start":
                stack.append(element)
                if record_tag is None:
                    selected_stack.append(len(stack) == 2)
                    if len(stack) == 2:
                        root_had_children = True
                else:
                    selected_stack.append(_tag_matches(element.tag, record_tag))
                    if selected_stack[-1] and any(selected_stack[:-1]):
                        raise XMLParseError(
                            f"XML record selector {record_tag!r} matches nested elements "
                            f"in {source_name}"
                        )
                continue

            selected = selected_stack.pop()
            depth = len(stack)
            is_root = depth == 1
            should_emit = selected or (record_tag is None and is_root and not root_had_children)
            if should_emit:
                index += 1
                record = XmlRecord(index=index, element=_snapshot(element))
                _clear_element(element)
                yield record
            elif record_tag is not None and not any(selected_stack):
                _clear_element(element)
            elif record_tag is None and depth > 2:
                pass
            elif record_tag is None and depth == 2:
                _clear_element(element)

            stack.pop()
            if record_tag is None and is_root:
                _clear_element(element)
    except etree.XMLSyntaxError as exc:
        raise XMLParseError(
            f"Malformed XML in {source_name} at line {exc.lineno}: {exc.msg}"
        ) from exc


def _tag_matches(tag: str, selector: str) -> bool:
    if tag == selector:
        return True
    return isinstance(tag, str) and tag.rsplit("}", 1)[-1] == selector


def _snapshot(element: etree._Element) -> XmlElement:
    children = tuple(_snapshot(child) for child in element if isinstance(child.tag, str))
    return XmlElement(
        tag=element.tag,
        attributes=tuple(element.attrib.items()),
        text=element.text,
        children=children,
        tail=element.tail,
    )


def _clear_element(element: etree._Element) -> None:
    element.clear(keep_tail=True)
    parent = element.getparent()
    if parent is not None:
        while element.getprevious() is not None:
            del parent[0]
