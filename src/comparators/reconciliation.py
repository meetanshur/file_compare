"""Streaming record-count and currency-total reconciliation."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation, localcontext
from typing import Any

from common.comparison_config import (
    ComparisonConfig,
    ReconciliationRules,
    Strictness,
    WhitespaceMode,
)
from common.comparison_result import ComparisonType, Difference, Severity
from comparators.records import parser_record_to_text_mapping
from parsers.xlsx import XLSXCellValue
from storage.duckdb_store import DuckDBStore, PersistResult


@dataclass(frozen=True)
class ReconciliationComparisonResult:
    """Counts, Decimal totals, and persisted findings for both input files."""

    records_file_a: int
    records_file_b: int
    bad_amounts_file_a: int
    bad_amounts_file_b: int
    totals_file_a: dict[str, dict[str, Decimal]]
    totals_file_b: dict[str, dict[str, Decimal]]
    differences: PersistResult

    @property
    def bad_amounts(self) -> int:
        """Return the number of malformed monetary values in both inputs."""
        return self.bad_amounts_file_a + self.bad_amounts_file_b


@dataclass
class _SideTotals:
    record_count: int = 0
    bad_amount_count: int = 0
    totals: dict[str, dict[str, Decimal]] = field(default_factory=dict)
    observed_fields: set[str] = field(default_factory=set)


class ReconciliationComparator:
    """Aggregate streamed records with exact Decimal arithmetic."""

    def compare(
        self,
        store: DuckDBStore,
        records_a: Iterable[object],
        records_b: Iterable[object],
        *,
        config: ComparisonConfig | None = None,
        cancellation_check: Callable[[], None] | None = None,
    ) -> ReconciliationComparisonResult:
        """Compare record counts and configured currency-wise monetary totals."""
        effective = config or ComparisonConfig()
        rules = effective.reconciliation_rules
        fields = _configured_fields(rules)
        pending_warnings: list[Difference] = []

        def persist_bad_amount_warning(warning: Difference) -> None:
            pending_warnings.append(warning)
            if len(pending_warnings) >= store.batch_size:
                store.insert_differences(pending_warnings)
                pending_warnings.clear()

        side_a = _aggregate_records(
            records_a, "A", rules, effective, cancellation_check, persist_bad_amount_warning
        )
        side_b = _aggregate_records(
            records_b, "B", rules, effective, cancellation_check, persist_bad_amount_warning
        )
        if pending_warnings:
            store.insert_differences(pending_warnings)

        observed_fields = side_a.observed_fields | side_b.observed_fields
        missing_fields = sorted(fields - observed_fields)
        if missing_fields and side_a.record_count + side_b.record_count:
            raise ValueError(
                "Configured reconciliation field(s) were not found in either input: "
                + ", ".join(missing_fields)
            )
        if cancellation_check is not None:
            cancellation_check()

        differences = store.insert_differences(
            _reconciliation_differences(side_a, side_b, effective, cancellation_check)
        )
        return ReconciliationComparisonResult(
            records_file_a=side_a.record_count,
            records_file_b=side_b.record_count,
            bad_amounts_file_a=side_a.bad_amount_count,
            bad_amounts_file_b=side_b.bad_amount_count,
            totals_file_a=side_a.totals,
            totals_file_b=side_b.totals,
            differences=differences,
        )


def _configured_fields(rules: ReconciliationRules) -> set[str]:
    return {
        field_name
        for field_name in (
            rules.currency_field,
            rules.amount_field,
            rules.debit_field,
            rules.credit_field,
        )
        if field_name is not None
    }


def _aggregate_records(
    records: Iterable[object],
    side: str,
    rules: ReconciliationRules,
    config: ComparisonConfig,
    cancellation_check: Callable[[], None] | None,
    bad_amount_callback: Callable[[Difference], None],
) -> _SideTotals:
    result = _SideTotals()
    metric_fields = {
        metric: field_name
        for metric, field_name in (
            ("amount", rules.amount_field),
            ("debit", rules.debit_field),
            ("credit", rules.credit_field),
        )
        if field_name is not None
    }
    configured_fields = _configured_fields(rules)

    iterator = iter(records)
    while True:
        batch = []
        for _ in range(1000):
            if cancellation_check is not None:
                cancellation_check()
            try:
                batch.append(next(iterator))
            except StopIteration:
                break
        if not batch:
            break
        for record in batch:
            values = _record_values(record, configured_fields)
            result.record_count += 1
            result.observed_fields.update(configured_fields.intersection(values))

            currency: str | None = None
            if rules.currency_field is not None:
                raw_currency = values.get(rules.currency_field, "")
                if raw_currency.strip():
                    currency = _normalize_currency(raw_currency, config)

            metric_values: dict[str, Decimal] = {}
            for metric, field_name in metric_fields.items():
                raw_value = values.get(field_name, "")
                try:
                    amount = Decimal(raw_value.strip())
                except InvalidOperation:
                    result.bad_amount_count += 1
                    bad_amount_callback(
                        _bad_amount_difference(
                            side,
                            _record_number(record, result.record_count),
                            field_name,
                            raw_value,
                            metric,
                        )
                    )
                    continue
                if not amount.is_finite():
                    result.bad_amount_count += 1
                    bad_amount_callback(
                        _bad_amount_difference(
                            side,
                            _record_number(record, result.record_count),
                            field_name,
                            raw_value,
                            metric,
                        )
                    )
                    continue
                metric_values[metric] = amount

            if metric_values and any(metric_values.values()):
                if currency is None:
                    raise ValueError(
                        f"Missing currency in file {side}, record {result.record_count}, "
                        f"field {rules.currency_field!r}"
                    )
            if currency is not None:
                currency_totals = result.totals.setdefault(
                    currency, {metric: Decimal(0) for metric in metric_fields}
                )
                for metric, amount in metric_values.items():
                    currency_totals[metric] = _add_exact(currency_totals[metric], amount)

    return result


def _record_number(record: object, fallback: int) -> int:
    for attribute in ("row_number", "index"):
        number = getattr(record, attribute, None)
        if isinstance(number, int):
            return number
    return fallback


def _bad_amount_difference(
    side: str, row_number: int, field_name: str, raw_value: str, metric: str
) -> Difference:
    display_value = raw_value if raw_value else "<empty>"
    return Difference(
        comparison_type=ComparisonType.RECONCILIATION,
        location=f"file={side} row={row_number} field={field_name}",
        message=(
            f"Malformed {metric} value {display_value!r}; excluded from reconciliation totals"
        ),
        severity=Severity.WARNING,
        value_a={"raw_value": raw_value},
        value_b=None,
    )


def _record_values(record: object, configured_fields: set[str]) -> dict[str, str]:
    values = parser_record_to_text_mapping(record)
    raw_values: Any = getattr(record, "values", None)
    if isinstance(raw_values, Mapping):
        for field_name in configured_fields:
            cell = raw_values.get(field_name)
            if isinstance(cell, XLSXCellValue):
                cell_value = cell.cached_value if cell.data_type == "formula" else cell.value
                values[field_name] = "" if cell_value is None else _text(cell_value)
    return values


def _text(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _normalize_currency(currency: str, config: ComparisonConfig) -> str:
    if config.whitespace is WhitespaceMode.TRIM:
        currency = currency.strip()
    elif config.whitespace is WhitespaceMode.COLLAPSE:
        currency = " ".join(currency.split())
    return currency if config.case_sensitive else currency.casefold()


def _add_exact(left: Decimal, right: Decimal) -> Decimal:
    """Add finite Decimals without rounding under the process-wide context."""
    left_tuple = left.as_tuple()
    right_tuple = right.as_tuple()
    common_exponent = min(left_tuple.exponent, right_tuple.exponent)
    left_digits = len(left_tuple.digits) + left_tuple.exponent - common_exponent
    right_digits = len(right_tuple.digits) + right_tuple.exponent - common_exponent
    with localcontext() as context:
        context.prec = max(context.prec, left_digits, right_digits) + 1
        return left + right


def _reconciliation_differences(
    side_a: _SideTotals,
    side_b: _SideTotals,
    config: ComparisonConfig,
    cancellation_check: Callable[[], None] | None,
) -> Iterable[Difference]:
    severity = Severity.ERROR if config.strictness is Strictness.STRICT else Severity.WARNING
    if side_a.record_count != side_b.record_count:
        if cancellation_check is not None:
            cancellation_check()
        yield Difference(
            comparison_type=ComparisonType.RECONCILIATION,
            location="record_count",
            message="Record counts differ",
            severity=severity,
            value_a={"record_count": side_a.record_count},
            value_b={"record_count": side_b.record_count},
        )

    currencies = sorted(set(side_a.totals) | set(side_b.totals))
    for currency in currencies:
        metrics = sorted(
            set(side_a.totals.get(currency, {})) | set(side_b.totals.get(currency, {}))
        )
        for metric in metrics:
            if cancellation_check is not None:
                cancellation_check()
            total_a = side_a.totals.get(currency, {}).get(metric, Decimal(0))
            total_b = side_b.totals.get(currency, {}).get(metric, Decimal(0))
            if abs(_add_exact(total_a, total_b.copy_negate())) <= config.numeric_tolerance:
                continue
            yield Difference(
                comparison_type=ComparisonType.RECONCILIATION,
                location=f"currency={currency} {metric}_total",
                message=f"{metric.capitalize()} totals differ for currency {currency!r}",
                severity=severity,
                value_a={f"{metric}_total": str(total_a)},
                value_b={f"{metric}_total": str(total_b)},
            )
