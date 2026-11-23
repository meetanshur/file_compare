"""Streaming keyed record comparison backed by DuckDB joins."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from common.comparison_config import ComparisonConfig
from parsers.structured import StructuredRecord
from parsers.xlsx import XLSXCellValue
from parsers.xml import XmlElement, XmlRecord
from storage.duckdb_store import DuckDBStore, PersistResult, RecordSide


@dataclass(frozen=True)
class RecordComparisonResult:
    """Counts and persistence status for one keyed record comparison."""

    records_file_a: int
    records_file_b: int
    differences: PersistResult


class RecordComparator:
    """Compare parser records by configured text keys using DuckDB."""

    def compare(
        self,
        store: DuckDBStore,
        records_a: Iterable[object],
        records_b: Iterable[object],
        *,
        key_fields: Iterable[str] | None = None,
        config: ComparisonConfig | None = None,
        trim_keys: bool | None = None,
        key_case_sensitive: bool | None = None,
        cancellation_check: Callable[[], None] | None = None,
    ) -> RecordComparisonResult:
        """Ingest parser records in batches, then persist SQL-joined findings."""
        keys = tuple(key_fields if key_fields is not None else config.record_key if config else ())
        if not keys:
            raise ValueError("At least one record key field must be configured")
        normalize_keys = config.trim_keys if trim_keys is None and config else bool(trim_keys)
        match_case = (
            config.key_case_sensitive
            if key_case_sensitive is None and config
            else key_case_sensitive is not False
        )

        count_a = store.insert_records(
            RecordSide.FILE_A,
            _text_records(records_a, cancellation_check),
            key_fields=keys,
            trim_keys=normalize_keys,
            key_case_sensitive=match_case,
            cancellation_check=cancellation_check,
        )
        count_b = store.insert_records(
            RecordSide.FILE_B,
            _text_records(records_b, cancellation_check),
            key_fields=keys,
            trim_keys=normalize_keys,
            key_case_sensitive=match_case,
            cancellation_check=cancellation_check,
        )
        if cancellation_check is not None:
            cancellation_check()
        store.create_record_key_index()
        differences = store.insert_differences(
            store.iter_record_comparison_differences(cancellation_check=cancellation_check)
        )
        return RecordComparisonResult(count_a, count_b, differences)


def _text_records(
    records: Iterable[object],
    cancellation_check: Callable[[], None] | None,
) -> Iterable[Mapping[str, str]]:
    for record in records:
        if cancellation_check is not None:
            cancellation_check()
        yield parser_record_to_text_mapping(record)


def parser_record_to_text_mapping(record: object) -> dict[str, str]:
    """Extract one parser record as text fields without normalizing field values."""
    if isinstance(record, Mapping):
        return _flatten_mapping(record)
    if isinstance(record, XmlRecord):
        return _xml_element_to_mapping(record.element)
    if isinstance(record, StructuredRecord) and isinstance(record.value, Mapping):
        return _flatten_mapping(record.value)
    value = getattr(record, "value", None)
    if isinstance(value, Mapping):
        return _flatten_mapping(value)
    values = getattr(record, "values", None)
    if isinstance(values, Mapping):
        mapped = _flatten_mapping(values)
        sheet_name = getattr(record, "sheet_name", None)
        if sheet_name is not None:
            mapped["@sheet_name"] = str(sheet_name)
        return mapped
    raise TypeError(f"Unsupported parser record type: {type(record).__name__}")


def _flatten_mapping(values: Mapping[str, Any]) -> dict[str, str]:
    result: dict[str, str] = {}
    for name, value in values.items():
        _flatten_value(str(name), value, result)
    return result


def _flatten_value(name: str, value: Any, result: dict[str, str]) -> None:
    if isinstance(value, XLSXCellValue):
        result[name] = _cell_text(value)
        if value.data_type == "formula":
            result[f"{name}.@cached_value"] = (
                "" if value.cached_value is None else _cell_text(value.cached_value)
            )
    elif isinstance(value, Mapping):
        for child_name, child_value in value.items():
            _flatten_value(f"{name}.{child_name}", child_value, result)
    elif isinstance(value, (list, tuple)):
        for index, child_value in enumerate(value):
            _flatten_value(f"{name}[{index}]", child_value, result)
    else:
        result[name] = _cell_text(value)


def _cell_text(value: Any) -> str:
    if isinstance(value, XLSXCellValue):
        if value.data_type == "formula":
            value = value.formula
        elif value.data_type == "blank":
            return ""
        else:
            value = value.value
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, (str, int, float, Decimal)):
        return str(value)
    raise TypeError(f"Parser field value has unsupported type: {type(value).__name__}")


def _xml_element_to_mapping(element: XmlElement) -> dict[str, str]:
    result: dict[str, str] = {}

    def visit(current: XmlElement, path: str) -> None:
        for name, value in current.attributes:
            field_name = f"{path}.@{_local_name(name)}" if path else f"@{_local_name(name)}"
            result[field_name] = value
        if current.text is not None:
            result[f"{path}.#text" if path else "#text"] = current.text
        elif not current.children:
            result[f"{path}.#text" if path else "#text"] = ""
        sibling_counts: dict[str, int] = {}
        for child in current.children:
            child_name = _local_name(child.tag)
            index = sibling_counts.get(child_name, 0)
            sibling_counts[child_name] = index + 1
            suffix = f"[{index}]" if index else ""
            child_path = f"{path}.{child_name}{suffix}" if path else f"{child_name}{suffix}"
            visit(child, child_path)

    visit(element, "")
    return result


def _local_name(name: str) -> str:
    return name.rsplit("}", 1)[-1]
