from __future__ import annotations

from decimal import Decimal

import pytest

from common.comparison_config import (
    ComparisonConfig,
    ReconciliationRules,
    Strictness,
    WhitespaceMode,
)
from common.comparison_result import ComparisonType, Severity
from comparators.reconciliation import ReconciliationComparator
from parsers.structured import StructuredRecord
from parsers.xlsx import XLSXCellValue, XLSXRecord
from storage.duckdb_store import DuckDBStore


def _compare(tmp_path, rows_a, rows_b, *, config=None, max_total_differences=100):
    store = DuckDBStore(
        tmp_path / "reconciliation.duckdb",
        batch_size=2,
        max_total_differences=max_total_differences,
    )
    result = ReconciliationComparator().compare(
        store,
        rows_a,
        rows_b,
        config=config or ComparisonConfig(),
    )
    return store, result


def test_reconciliation_compares_record_counts(tmp_path) -> None:
    store, result = _compare(tmp_path, [{"id": "1"}, {"id": "2"}], [{"id": "1"}])
    try:
        findings = list(store.iter_differences(comparison_type=ComparisonType.RECONCILIATION))
    finally:
        store.close()

    assert (result.records_file_a, result.records_file_b) == (2, 1)
    assert len(findings) == 1
    assert findings[0].location == "record_count"
    assert findings[0].value_a == {"record_count": 2}
    assert findings[0].value_b == {"record_count": 1}


def test_reconciliation_aggregates_currency_totals_with_decimal(tmp_path) -> None:
    config = ComparisonConfig(
        reconciliation_rules=ReconciliationRules(currency_field="currency", amount_field="amount")
    )
    store, result = _compare(
        tmp_path,
        [
            {"currency": "USD", "amount": "0.10"},
            {"currency": "USD", "amount": "0.20"},
            {"currency": "EUR", "amount": "2.00"},
        ],
        [{"currency": "USD", "amount": "0.31"}, {"currency": "EUR", "amount": "2.00"}],
        config=config,
    )
    try:
        findings = list(store.iter_differences(comparison_type=ComparisonType.RECONCILIATION))
    finally:
        store.close()

    assert result.totals_file_a["USD"]["amount"] == Decimal("0.30")
    assert result.totals_file_b["USD"]["amount"] == Decimal("0.31")
    assert result.totals_file_a["EUR"]["amount"] == Decimal("2.00")
    amount_difference = next(item for item in findings if "currency=USD" in item.location)
    assert amount_difference.value_a == {"amount_total": "0.30"}
    assert amount_difference.value_b == {"amount_total": "0.31"}


def test_reconciliation_sums_more_than_default_decimal_precision(tmp_path) -> None:
    total = "123456789012345678901234567890.13"
    config = ComparisonConfig(
        reconciliation_rules=ReconciliationRules(currency_field="currency", amount_field="amount")
    )
    store, result = _compare(
        tmp_path,
        [
            {"currency": "USD", "amount": "123456789012345678901234567890.12"},
            {"currency": "USD", "amount": "0.01"},
        ],
        [
            {"currency": "USD", "amount": total},
            {"currency": "USD", "amount": "0.00"},
        ],
        config=config,
    )
    try:
        findings = list(store.iter_differences(comparison_type=ComparisonType.RECONCILIATION))
    finally:
        store.close()

    assert result.totals_file_a["USD"]["amount"] == Decimal(total)
    assert findings == []


def test_reconciliation_aggregates_debit_and_credit_by_currency(tmp_path) -> None:
    config = ComparisonConfig(
        reconciliation_rules=ReconciliationRules(
            currency_field="currency",
            debit_field="debit",
            credit_field="credit",
        )
    )
    store, result = _compare(
        tmp_path,
        [{"currency": "USD", "debit": "5.25", "credit": "0.00"}],
        [{"currency": "USD", "debit": "5.00", "credit": "0.25"}],
        config=config,
    )
    try:
        findings = list(store.iter_differences(comparison_type=ComparisonType.RECONCILIATION))
    finally:
        store.close()

    assert result.totals_file_a["USD"] == {
        "debit": Decimal("5.25"),
        "credit": Decimal("0"),
    }
    assert result.totals_file_b["USD"] == {
        "debit": Decimal("5.00"),
        "credit": Decimal("0.25"),
    }
    assert {item.location for item in findings} == {
        "currency=USD credit_total",
        "currency=USD debit_total",
    }


def test_reconciliation_keeps_three_currency_totals_separate(tmp_path) -> None:
    config = ComparisonConfig(
        reconciliation_rules=ReconciliationRules(
            currency_field="currency",
            debit_field="debit",
            credit_field="credit",
        )
    )
    rows_a = [
        {"currency": "USD", "debit": "10.00", "credit": "2.00"},
        {"currency": "EUR", "debit": "20.00", "credit": "3.00"},
        {"currency": "JPY", "debit": "30.00", "credit": "4.00"},
    ]
    rows_b = [
        {"currency": "USD", "debit": "10.01", "credit": "1.99"},
        {"currency": "EUR", "debit": "20.00", "credit": "3.00"},
        {"currency": "JPY", "debit": "30.00", "credit": "4.00"},
    ]
    store, result = _compare(tmp_path, rows_a, rows_b, config=config)
    try:
        findings = list(store.iter_differences(comparison_type=ComparisonType.RECONCILIATION))
    finally:
        store.close()

    assert set(result.totals_file_a) == {"USD", "EUR", "JPY"}
    assert result.totals_file_a["USD"]["debit"] == Decimal("10.00")
    assert result.totals_file_a["EUR"]["debit"] == Decimal("20.00")
    assert result.totals_file_a["JPY"]["debit"] == Decimal("30.00")
    assert {item.location for item in findings} == {
        "currency=USD debit_total",
        "currency=USD credit_total",
    }


def test_reconciliation_honors_tolerance_and_currency_normalization(tmp_path) -> None:
    config = ComparisonConfig(
        reconciliation_rules=ReconciliationRules(currency_field="currency", amount_field="amount"),
        numeric_tolerance=Decimal("0.01"),
        case_sensitive=False,
        whitespace=WhitespaceMode.TRIM,
    )
    store, _ = _compare(
        tmp_path,
        [{"currency": " USD ", "amount": "1.00"}],
        [{"currency": "usd", "amount": "1.01"}],
        config=config,
    )
    try:
        findings = list(store.iter_differences(comparison_type=ComparisonType.RECONCILIATION))
    finally:
        store.close()

    assert findings == []


@pytest.mark.parametrize("bad_value", ["abc", "", "1,000.00", "1.2.3"])
def test_reconciliation_warns_for_bad_amounts_and_excludes_them(tmp_path, bad_value) -> None:
    config = ComparisonConfig(
        reconciliation_rules=ReconciliationRules(currency_field="currency", amount_field="amount")
    )
    rows = [
        StructuredRecord(index=7, value={"currency": "USD", "amount": "2.50"}),
        StructuredRecord(index=8, value={"currency": "USD", "amount": bad_value}),
    ]
    store, result = _compare(tmp_path, rows, [], config=config)
    try:
        findings = list(store.iter_differences(comparison_type=ComparisonType.RECONCILIATION))
    finally:
        store.close()

    bad_amount_warning = next(item for item in findings if "Malformed amount" in item.message)
    assert result.bad_amounts == 1
    assert result.bad_amounts_file_a == 1
    assert result.bad_amounts_file_b == 0
    assert result.totals_file_a["USD"]["amount"] == Decimal("2.50")
    assert bad_amount_warning.severity is Severity.WARNING
    assert bad_amount_warning.location == "file=A row=8 field=amount"
    assert bad_amount_warning.value_a == {"raw_value": bad_value}


def test_reconciliation_warns_and_excludes_non_finite_amounts(tmp_path) -> None:
    config = ComparisonConfig(
        reconciliation_rules=ReconciliationRules(currency_field="currency", amount_field="amount")
    )
    store, result = _compare(
        tmp_path,
        [{"currency": "USD", "amount": "NaN"}],
        [],
        config=config,
    )
    try:
        findings = list(store.iter_differences(comparison_type=ComparisonType.RECONCILIATION))
    finally:
        store.close()

    assert result.bad_amounts == 1
    assert result.totals_file_a["USD"]["amount"] == Decimal("0")
    assert any("Malformed amount" in item.message for item in findings)


def test_reconciliation_rejects_nonzero_amount_without_currency(tmp_path) -> None:
    config = ComparisonConfig(
        reconciliation_rules=ReconciliationRules(currency_field="currency", amount_field="amount")
    )
    store = DuckDBStore(tmp_path / "missing-currency.duckdb")
    try:
        with pytest.raises(ValueError, match="Missing currency"):
            ReconciliationComparator().compare(store, [{"amount": "1.00"}], [], config=config)
    finally:
        store.close()


def test_reconciliation_requires_configured_fields_to_exist(tmp_path) -> None:
    config = ComparisonConfig(
        reconciliation_rules=ReconciliationRules(
            currency_field="currency",
            amount_field="misspelled_amount",
        )
    )
    store = DuckDBStore(tmp_path / "missing-field.duckdb")
    try:
        with pytest.raises(ValueError, match="misspelled_amount"):
            ReconciliationComparator().compare(
                store,
                [{"currency": "USD", "amount": "1.00"}],
                [{"currency": "USD", "amount": "1.00"}],
                config=config,
            )
    finally:
        store.close()


def test_reconciliation_uses_xlsx_formula_cached_values(tmp_path) -> None:
    config = ComparisonConfig(
        reconciliation_rules=ReconciliationRules(currency_field="currency", amount_field="amount")
    )
    rows_a = [
        XLSXRecord(
            index=1,
            sheet_name="Ledger",
            row_number=2,
            values={
                "currency": XLSXCellValue("USD", "string", "@"),
                "amount": XLSXCellValue(
                    value=None,
                    data_type="formula",
                    number_format="0.00",
                    formula="=SUM(B2:B3)",
                    cached_value=Decimal("3.50"),
                ),
            },
        )
    ]
    store, result = _compare(tmp_path, rows_a, rows_a, config=config)
    try:
        findings = list(store.iter_differences(comparison_type=ComparisonType.RECONCILIATION))
    finally:
        store.close()

    assert result.totals_file_a["USD"]["amount"] == Decimal("3.50")
    assert findings == []


def test_reconciliation_uses_strictness_and_difference_limit(tmp_path) -> None:
    config = ComparisonConfig(
        reconciliation_rules=ReconciliationRules(currency_field="currency", amount_field="amount"),
        strictness=Strictness.LENIENT,
    )
    store, result = _compare(
        tmp_path,
        [{"currency": "USD", "amount": "1"}, {"currency": "EUR", "amount": "2"}],
        [{"currency": "USD", "amount": "2"}, {"currency": "EUR", "amount": "3"}],
        config=config,
        max_total_differences=1,
    )
    try:
        findings = list(store.iter_differences(comparison_type=ComparisonType.RECONCILIATION))
    finally:
        store.close()

    assert len(findings) == 1
    assert findings[0].severity is Severity.WARNING
    assert result.differences.truncated is True


def test_reconciliation_checks_cancellation_while_streaming(tmp_path) -> None:
    calls = 0

    def cancellation_check() -> None:
        nonlocal calls
        calls += 1
        if calls == 3:
            raise RuntimeError("cancelled")

    store = DuckDBStore(tmp_path / "cancel.duckdb")
    try:
        with pytest.raises(RuntimeError, match="cancelled"):
            ReconciliationComparator().compare(
                store,
                ({"id": str(index)} for index in range(10)),
                [],
                cancellation_check=cancellation_check,
            )
    finally:
        store.close()
