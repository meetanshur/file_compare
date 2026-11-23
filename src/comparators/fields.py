"""Streaming field-name, value, and parser-type comparison."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from common.comparison_config import ComparisonConfig, Strictness
from common.comparison_result import Severity
from common.comparison_rules import ComparisonRules
from comparators.records import parser_record_to_text_mapping
from parsers.structured import JSONNumber, StructuredRecord
from parsers.xlsx import XLSXCellValue
from parsers.xml import XmlRecord
from storage.duckdb_store import DuckDBStore, PersistResult, RecordSide


@dataclass(frozen=True)
class FieldComparisonResult:
    """Record counts and bounded finding-persistence outcome."""

    records_file_a: int
    records_file_b: int
    differences: PersistResult


@dataclass(frozen=True)
class _TypedField:
    value: str
    data_type: str
    source_type: str
    comparison_value: str


@dataclass(frozen=True)
class _TypedRecord:
    fields: Mapping[str, _TypedField]


class FieldComparator:
    """Compare parser fields using text values and retained parser type metadata."""

    def compare(
        self,
        store: DuckDBStore,
        records_a: Iterable[object],
        records_b: Iterable[object],
        *,
        key_fields: Iterable[str] | None = None,
        config: ComparisonConfig | None = None,
        cancellation_check: Callable[[], None] | None = None,
    ) -> FieldComparisonResult:
        """Stream typed records into DuckDB and persist field-level findings."""
        effective = config or ComparisonConfig()
        rules = ComparisonRules(effective)
        keys = tuple(key_fields if key_fields is not None else effective.record_key)
        if not keys:
            raise ValueError("At least one record key field must be configured")

        store.reset_field_records()
        count_a = store.insert_field_records(
            RecordSide.FILE_A,
            _typed_records(records_a, effective, cancellation_check),
            key_fields=keys,
            trim_keys=effective.trim_keys,
            key_case_sensitive=effective.key_case_sensitive,
            cancellation_check=cancellation_check,
        )
        count_b = store.insert_field_records(
            RecordSide.FILE_B,
            _typed_records(records_b, effective, cancellation_check),
            key_fields=keys,
            trim_keys=effective.trim_keys,
            key_case_sensitive=effective.key_case_sensitive,
            cancellation_check=cancellation_check,
        )
        if cancellation_check is not None:
            cancellation_check()
        store.create_field_record_indexes()
        differences = store.insert_differences(
            store.iter_field_comparison_differences(
                comparison_rules=rules,
                severity=(
                    Severity.ERROR
                    if effective.strictness is Strictness.STRICT
                    else Severity.WARNING
                ),
                cancellation_check=cancellation_check,
            )
        )
        return FieldComparisonResult(count_a, count_b, differences)


def _typed_records(
    records: Iterable[object],
    config: ComparisonConfig,
    cancellation_check: Callable[[], None] | None,
) -> Iterable[_TypedRecord]:
    for record in records:
        if cancellation_check is not None:
            cancellation_check()
        yield _to_typed_record(record, config)


def _to_typed_record(record: object, config: ComparisonConfig) -> _TypedRecord:
    if isinstance(record, StructuredRecord) and isinstance(record.value, Mapping):
        source_type = "json"
        values = record.value
        fields = _flatten_typed(values, source_type, config)
    elif isinstance(record, XmlRecord):
        fields = _xml_typed_fields(record, config)
    else:
        values = getattr(record, "values", None)
        if isinstance(values, Mapping):
            source_type = (
                "xlsx"
                if values and any(isinstance(value, XLSXCellValue) for value in values.values())
                else "text"
            )
            fields = _flatten_typed(values, source_type, config)
            sheet_name = getattr(record, "sheet_name", None)
            if sheet_name is not None:
                fields["@sheet_name"] = _typed_field(
                    str(sheet_name), "string", "xlsx", config, "@sheet_name"
                )
        elif isinstance(record, Mapping):
            fields = _flatten_typed(record, "text", config)
        else:
            fields = _flatten_typed(parser_record_to_text_mapping(record), "text", config)
    return _TypedRecord(fields)


def _flatten_typed(
    values: Mapping[str, Any],
    source_type: str,
    config: ComparisonConfig,
) -> dict[str, _TypedField]:
    fields: dict[str, _TypedField] = {}
    for name, value in values.items():
        _flatten_value(str(name), value, source_type, config, fields)
    return fields


def _flatten_value(
    name: str,
    value: Any,
    source_type: str,
    config: ComparisonConfig,
    fields: dict[str, _TypedField],
) -> None:
    if isinstance(value, XLSXCellValue):
        if value.data_type == "formula":
            fields[name] = _typed_field(value.formula or "", "formula", "xlsx", config, name)
            fields[f"{name}.@cached_value"] = _typed_field(
                "" if value.cached_value is None else _text(value.cached_value),
                "cached_value",
                "xlsx",
                config,
                f"{name}.@cached_value",
            )
        else:
            cell_type = "blank" if value.data_type == "blank" else value.data_type
            cell_value = "" if value.data_type == "blank" else _text(value.value)
            fields[name] = _typed_field(cell_value, cell_type, "xlsx", config, name)
    elif isinstance(value, Mapping):
        for child_name, child_value in value.items():
            _flatten_value(f"{name}.{child_name}", child_value, source_type, config, fields)
    elif isinstance(value, (list, tuple)):
        for index, child_value in enumerate(value):
            _flatten_value(f"{name}[{index}]", child_value, source_type, config, fields)
    else:
        data_type = _inferred_type(value, source_type)
        fields[name] = _typed_field(_text(value), data_type, source_type, config, name)


def _xml_typed_fields(record: XmlRecord, config: ComparisonConfig) -> dict[str, _TypedField]:
    from comparators.records import parser_record_to_text_mapping

    rules = ComparisonRules(config)
    return {
        name: _TypedField(value, "text", "xml", rules.normalize_text(value, name))
        for name, value in parser_record_to_text_mapping(record).items()
    }


def _inferred_type(value: Any, source_type: str) -> str:
    if source_type == "json":
        if isinstance(value, JSONNumber):
            return "number"
        if isinstance(value, bool):
            return "boolean"
        if value is None:
            return "null"
        return "string"
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float, Decimal)):
        return "number"
    if isinstance(value, (date, datetime)):
        return "date"
    return "text" if source_type == "text" else "string"


def _typed_field(
    value: str, data_type: str, source_type: str, config: ComparisonConfig, field_name: str
) -> _TypedField:
    rules = ComparisonRules(config)
    return _TypedField(
        value=value,
        data_type=data_type,
        source_type=source_type,
        comparison_value=rules.normalize_text(value, field_name),
    )


def _text(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)
