"""Shared field-rule resolution and value comparison helpers."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, InvalidOperation, localcontext
from fnmatch import fnmatchcase

from common.comparison_config import ComparisonConfig, FieldRuleOverride, WhitespaceMode


class ComparisonRules:
    """Resolve and apply validated comparison rules consistently."""

    def __init__(self, config: ComparisonConfig) -> None:
        self.config = config

    @property
    def ignores_element_order(self) -> bool:
        """Whether XML sibling order is ignored by the selected rule preset."""
        return self.config.ignore_element_order

    def override(self, field_name: str) -> FieldRuleOverride | None:
        return self.config.field_override(field_name)

    def is_ignored(self, field_name: str) -> bool:
        override = self.override(field_name)
        if override is not None and override.ignore is not None:
            return override.ignore
        return any(fnmatchcase(field_name, pattern) for pattern in self.config.ignore_fields)

    def option(self, field_name: str, name: str):
        override = self.override(field_name)
        value = getattr(override, name) if override is not None else None
        return getattr(self.config, name) if value is None else value

    def normalize_text(self, value: str, field_name: str) -> str:
        whitespace = self.option(field_name, "whitespace")
        if whitespace is WhitespaceMode.TRIM:
            value = value.strip()
        elif whitespace is WhitespaceMode.COLLAPSE:
            value = " ".join(value.split())
        if not self.option(field_name, "case_sensitive"):
            value = value.casefold()
        return value

    def parse_date(self, value: str) -> date | None:
        for date_format in self.config.date_formats:
            try:
                return datetime.strptime(value, date_format).date()
            except ValueError:
                continue
        return None

    def equal_date(self, value_a: str, value_b: str) -> bool:
        date_a = self.parse_date(value_a)
        date_b = self.parse_date(value_b)
        return date_a is not None and date_b is not None and date_a == date_b

    def is_date_format_difference(self, value_a: str, value_b: str) -> bool:
        return (
            value_a != value_b
            and self.parse_date(value_a) is not None
            and self.equal_date(value_a, value_b)
        )

    def equal_numbers(self, field_name: str, value_a: str, value_b: str) -> bool:
        try:
            amount_a = Decimal(value_a.strip())
            amount_b = Decimal(value_b.strip())
        except InvalidOperation:
            return False
        if not amount_a.is_finite() or not amount_b.is_finite():
            return False
        digits = max(len(amount_a.as_tuple().digits), len(amount_b.as_tuple().digits))
        with localcontext() as context:
            context.prec = max(context.prec, digits + 2)
            difference = abs(amount_a - amount_b)
        return difference <= self.option(field_name, "numeric_tolerance")

    def equal_values(
        self,
        field_name: str,
        value_a: str,
        value_b: str,
        *,
        type_a: str | None = None,
        type_b: str | None = None,
    ) -> bool:
        if type_a is not None and type_b is not None and type_a != type_b:
            if not (
                self.option(field_name, "null_equals_empty")
                and {type_a, type_b} <= {"null", "string", "text"}
                and (type_a == "null" and value_b == "" or type_b == "null" and value_a == "")
            ):
                return False
        if self.option(field_name, "null_equals_empty") and (
            value_a == "null" and value_b == "" or value_b == "null" and value_a == ""
        ):
            return True

        tolerance = self.option(field_name, "numeric_tolerance")
        if tolerance:
            try:
                amount_a = Decimal(value_a)
                amount_b = Decimal(value_b)
            except InvalidOperation:
                pass
            else:
                if amount_a.is_finite() and amount_b.is_finite():
                    return self.equal_numbers(field_name, value_a, value_b)

        if self.option(field_name, "treat_equal_dates_as_same"):
            if self.equal_date(value_a, value_b):
                return True

        return self.normalize_text(value_a, field_name) == self.normalize_text(value_b, field_name)

    def normalize_record(self, record: dict[str, str]) -> dict[str, str]:
        """Create a comparison-only record with ignored and normalized fields removed."""
        normalized: dict[str, str] = {}
        for field_name, value in record.items():
            if self.is_ignored(field_name):
                continue
            if self.option(field_name, "null_equals_empty") and value == "null":
                value = ""
            normalized[field_name] = self.normalize_text(value, field_name)
        return normalized

    def records_equal(self, record_a: dict[str, str], record_b: dict[str, str]) -> bool:
        normalized_fields_a = {
            field_name for field_name in record_a if not self.is_ignored(field_name)
        }
        normalized_fields_b = {
            field_name for field_name in record_b if not self.is_ignored(field_name)
        }
        if normalized_fields_a != normalized_fields_b:
            return False
        return all(
            self.equal_values(
                field_name,
                str(record_a[field_name]),
                str(record_b[field_name]),
            )
            for field_name in normalized_fields_a
        )
