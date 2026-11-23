"""Row-level financial comparison using shared comparison rules."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from itertools import zip_longest

from common.comparison_config import ComparisonConfig, Strictness
from common.comparison_result import ComparisonType, Difference, Severity
from common.comparison_rules import ComparisonRules
from comparators.records import parser_record_to_text_mapping
from parsers.xlsx import XLSXCellValue
from storage.duckdb_store import DuckDBStore, PersistResult


@dataclass(frozen=True)
class FinancialComparisonResult:
    """Record counts and bounded financial finding persistence outcome."""

    records_file_a: int
    records_file_b: int
    differences: PersistResult


class FinancialComparator:
    """Compare configured currency and amount fields by streamed row position."""

    def compare(
        self,
        store: DuckDBStore,
        records_a: Iterable[object],
        records_b: Iterable[object],
        *,
        config: ComparisonConfig | None = None,
        cancellation_check: Callable[[], None] | None = None,
    ) -> FinancialComparisonResult:
        """Report currency, amount, precision, and sign findings separately."""
        effective = config or ComparisonConfig()
        rules = ComparisonRules(effective)
        severity = Severity.ERROR if effective.strictness is Strictness.STRICT else Severity.WARNING
        missing = object()
        count_a = 0
        count_b = 0

        def findings() -> Iterable[Difference]:
            nonlocal count_a, count_b
            for row_number, (record_a, record_b) in enumerate(
                zip_longest(records_a, records_b, fillvalue=missing), start=1
            ):
                if cancellation_check is not None:
                    cancellation_check()
                if record_a is missing:
                    count_b += 1
                    continue
                if record_b is missing:
                    count_a += 1
                    continue
                count_a += 1
                count_b += 1
                yield from _compare_financial_row(
                    _financial_values(record_a, effective),
                    _financial_values(record_b, effective),
                    row_number,
                    effective,
                    rules,
                    severity,
                )

        result = store.insert_differences(findings())
        return FinancialComparisonResult(count_a, count_b, result)


def _financial_values(record: object, config: ComparisonConfig) -> dict[str, str]:
    values = parser_record_to_text_mapping(record)
    record_values = getattr(record, "values", None)
    if not isinstance(record_values, Mapping):
        return values
    fields = {
        config.financial_rules.currency_field,
        config.financial_rules.amount_field,
    }
    for field_name in fields:
        if field_name is None:
            continue
        cell = record_values.get(field_name)
        if isinstance(cell, XLSXCellValue):
            cell_value = cell.cached_value if cell.data_type == "formula" else cell.value
            values[field_name] = "" if cell_value is None else str(cell_value)
    return values


def _compare_financial_row(
    values_a: dict[str, str],
    values_b: dict[str, str],
    row_number: int,
    config: ComparisonConfig,
    rules: ComparisonRules,
    severity: Severity,
) -> Iterable[Difference]:
    financial = config.financial_rules
    currency_field = financial.currency_field
    amount_field = financial.amount_field

    if currency_field and not rules.is_ignored(currency_field):
        currency_a = values_a.get(currency_field, "")
        currency_b = values_b.get(currency_field, "")
        if not rules.equal_values(currency_field, currency_a, currency_b):
            yield Difference(
                comparison_type=ComparisonType.FINANCIAL,
                location=f"row={row_number} field={currency_field}",
                message="Currency differs",
                severity=severity,
                value_a=currency_a,
                value_b=currency_b,
            )

    if amount_field is None or rules.is_ignored(amount_field):
        return
    raw_a = values_a.get(amount_field, "")
    raw_b = values_b.get(amount_field, "")
    try:
        amount_a = Decimal(raw_a.strip())
        amount_b = Decimal(raw_b.strip())
    except InvalidOperation as exc:
        raise ValueError(
            f"Invalid financial amount at row {row_number}, field {amount_field!r}"
        ) from exc
    if not amount_a.is_finite() or not amount_b.is_finite():
        raise ValueError(f"Non-finite financial amount at row {row_number}, field {amount_field!r}")

    if not rules.equal_numbers(amount_field, raw_a, raw_b):
        yield Difference(
            comparison_type=ComparisonType.FINANCIAL,
            location=f"row={row_number} field={amount_field}",
            message="Amount differs",
            severity=severity,
            value_a=raw_a,
            value_b=raw_b,
        )
    if financial.check_decimal_precision and _decimal_places(raw_a) != _decimal_places(raw_b):
        yield Difference(
            comparison_type=ComparisonType.FINANCIAL,
            location=f"row={row_number} field={amount_field}.precision",
            message="Decimal precision differs",
            severity=severity,
            value_a=_decimal_places(raw_a),
            value_b=_decimal_places(raw_b),
        )
    sign_a = _sign(amount_a)
    sign_b = _sign(amount_b)
    if financial.check_sign and sign_a != sign_b:
        yield Difference(
            comparison_type=ComparisonType.FINANCIAL,
            location=f"row={row_number} field={amount_field}.sign",
            message="Amount sign differs",
            severity=severity,
            value_a=sign_a,
            value_b=sign_b,
        )


def _decimal_places(value: str) -> int:
    try:
        amount = Decimal(value.strip())
    except InvalidOperation as exc:
        raise ValueError(f"Invalid financial amount: {value!r}") from exc
    return max(0, -amount.as_tuple().exponent)


def _sign(amount: Decimal) -> str:
    if amount == 0:
        return "zero"
    return "negative" if amount < 0 else "positive"
