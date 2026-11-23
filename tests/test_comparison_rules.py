from __future__ import annotations

import io
from decimal import Decimal

import pytest

from common.comparison_config import (
    ComparisonConfig,
    FieldRuleOverride,
    FinancialRules,
    Strictness,
    WhitespaceMode,
    load_comparison_config,
)
from common.comparison_result import ComparisonType
from comparators.fields import FieldComparator
from comparators.financial import FinancialComparator
from comparators.raw import RawComparator
from comparators.records import RecordComparator
from comparators.xml_schema import XMLSchemaComparator
from parsers.xml import XmlElement, XmlRecord
from storage.duckdb_store import DuckDBStore


def _field_differences(tmp_path, rows_a, rows_b, config):
    store = DuckDBStore(tmp_path / "fields.duckdb")
    try:
        FieldComparator().compare(store, rows_a, rows_b, config=config)
        return list(store.iter_differences(comparison_type=ComparisonType.FIELD))
    finally:
        store.close()


def test_strict_defaults_and_lenient_preset_allow_individual_overrides() -> None:
    strict = ComparisonConfig()
    lenient = ComparisonConfig(
        strictness=Strictness.LENIENT,
        case_sensitive=True,
        numeric_tolerance=Decimal("0"),
    )

    assert strict.ignore_fields == []
    assert strict.null_equals_empty is False
    assert strict.case_sensitive is True
    assert strict.whitespace is WhitespaceMode.PRESERVE
    assert strict.ignore_element_order is False
    assert strict.treat_equal_dates_as_same is False
    assert strict.numeric_tolerance == Decimal("0")
    assert lenient.null_equals_empty is True
    assert lenient.case_sensitive is True
    assert lenient.whitespace is WhitespaceMode.TRIM
    assert lenient.ignore_element_order is True
    assert lenient.treat_equal_dates_as_same is True
    assert lenient.numeric_tolerance == Decimal("0")


def test_ignore_patterns_and_per_field_overrides_apply_to_fields(tmp_path) -> None:
    config = ComparisonConfig(
        record_key=["id"],
        ignore_fields=["*_timestamp"],
        field_overrides={
            "email": FieldRuleOverride(case_sensitive=False, ignore=False),
            "secret": FieldRuleOverride(ignore=True),
        },
    )
    findings = _field_differences(
        tmp_path,
        [
            {"id": "1", "created_timestamp": "old", "email": "A@EXAMPLE.COM", "secret": "a"},
        ],
        [
            {"id": "1", "created_timestamp": "new", "email": "a@example.com", "secret": "b"},
        ],
        config,
    )

    assert findings == []


def test_xml_attribute_name_without_at_prefix_is_accepted_as_record_key(tmp_path) -> None:
    record_a = XmlRecord(
        index=1,
        element=XmlElement(
            tag="record",
            attributes=(("id", "007"),),
            text=None,
            children=(XmlElement("value", (), "old", (), None),),
            tail=None,
        ),
    )
    record_b = XmlRecord(
        index=1,
        element=XmlElement(
            tag="record",
            attributes=(("id", "007"),),
            text=None,
            children=(XmlElement("value", (), "new", (), None),),
            tail=None,
        ),
    )
    config = ComparisonConfig(record_key=["id"])

    with DuckDBStore(tmp_path / "xml-key.duckdb") as store:
        RecordComparator().compare(store, [record_a], [record_b], config=config)
        record_findings = list(store.iter_differences())

    with DuckDBStore(tmp_path / "xml-field-key.duckdb") as store:
        FieldComparator().compare(store, [record_a], [record_b], config=config)
        field_findings = list(store.iter_differences(comparison_type=ComparisonType.FIELD))

    assert len(record_findings) == 1
    assert record_findings[0].comparison_type is ComparisonType.UPDATE
    assert field_findings


def test_per_field_null_and_numeric_tolerance_overrides(tmp_path) -> None:
    config = ComparisonConfig(
        record_key=["id"],
        field_overrides={
            "optional": FieldRuleOverride(null_equals_empty=True),
            "amount": FieldRuleOverride(numeric_tolerance=Decimal("0.05")),
        },
    )
    findings = _field_differences(
        tmp_path,
        [{"id": "1", "optional": None, "amount": "10.00"}],
        [{"id": "1", "optional": "", "amount": "10.03"}],
        config,
    )

    assert findings == []


def test_config_errors_identify_unknown_rule_and_wrong_rule_type(tmp_path) -> None:
    unknown_path = tmp_path / "unknown.yaml"
    unknown_path.write_text(
        "field_overrides:\n  amount:\n    numeric_tolerence: 0.1\n", encoding="utf-8"
    )
    with pytest.raises(ValueError, match=r"field_overrides.amount.numeric_tolerence"):
        load_comparison_config(unknown_path)

    wrong_type_path = tmp_path / "wrong-type.json"
    wrong_type_path.write_text('{"case_sensitive": "not-a-bool"}', encoding="utf-8")
    with pytest.raises(ValueError, match="case_sensitive"):
        load_comparison_config(wrong_type_path)


def test_equal_dates_get_separate_format_finding_and_can_be_treated_equal(tmp_path) -> None:
    rows_a = [{"id": "1", "date": "2026-11-24"}]
    rows_b = [{"id": "1", "date": "24/11/2026"}]
    strict = _field_differences(
        tmp_path / "strict",
        rows_a,
        rows_b,
        ComparisonConfig(record_key=["id"]),
    )
    lenient = _field_differences(
        tmp_path / "lenient",
        rows_a,
        rows_b,
        ComparisonConfig(
            record_key=["id"],
            field_overrides={"date": FieldRuleOverride(treat_equal_dates_as_same=True)},
        ),
    )

    assert any("Date format differs" in item.message for item in strict)
    assert any("differs in value or data type" in item.message for item in strict)
    assert len(lenient) == 1
    assert "Date format differs" in lenient[0].message


def test_record_comparison_uses_shared_ignore_and_whitespace_rules(tmp_path) -> None:
    store = DuckDBStore(tmp_path / "records.duckdb")
    try:
        RecordComparator().compare(
            store,
            [{"id": "1", "value": "same ", "build_timestamp": "one"}],
            [{"id": "1", "value": "same", "build_timestamp": "two"}],
            config=ComparisonConfig(
                record_key=["id"],
                ignore_fields=["*_timestamp"],
                whitespace=WhitespaceMode.TRIM,
            ),
        )
        findings = list(store.iter_differences())
    finally:
        store.close()

    assert findings == []


def test_financial_findings_are_separate_and_tolerances_are_field_specific(tmp_path) -> None:
    store = DuckDBStore(tmp_path / "financial.duckdb")
    try:
        FinancialComparator().compare(
            store,
            [
                {"currency": "USD", "amount": "-10.5"},
                {"currency": "EUR", "amount": "8.00"},
            ],
            [
                {"currency": "EUR", "amount": "10.50"},
                {"currency": "EUR", "amount": "8.04"},
            ],
            config=ComparisonConfig(
                financial_rules=FinancialRules(currency_field="currency", amount_field="amount"),
                field_overrides={"amount": FieldRuleOverride(numeric_tolerance=Decimal("0.05"))},
            ),
        )
        findings = list(store.iter_differences(comparison_type=ComparisonType.FINANCIAL))
    finally:
        store.close()

    assert {item.message for item in findings} == {
        "Currency differs",
        "Amount differs",
        "Decimal precision differs",
        "Amount sign differs",
    }


def test_financial_amount_diff_uses_exact_default_and_reports_sign(tmp_path) -> None:
    store = DuckDBStore(tmp_path / "financial-exact.duckdb")
    try:
        FinancialComparator().compare(
            store,
            [{"currency": "USD", "amount": "-5.00"}],
            [{"currency": "USD", "amount": "5.00"}],
            config=ComparisonConfig(
                financial_rules=FinancialRules(currency_field="currency", amount_field="amount")
            ),
        )
        findings = list(store.iter_differences(comparison_type=ComparisonType.FINANCIAL))
    finally:
        store.close()

    assert {item.message for item in findings} == {
        "Amount differs",
        "Amount sign differs",
    }


def test_financial_precision_is_separate_from_numeric_amount_difference(tmp_path) -> None:
    store = DuckDBStore(tmp_path / "financial-precision.duckdb")
    try:
        FinancialComparator().compare(
            store,
            [{"currency": "USD", "amount": "10.5"}],
            [{"currency": "USD", "amount": "10.50"}],
            config=ComparisonConfig(
                financial_rules=FinancialRules(currency_field="currency", amount_field="amount")
            ),
        )
        findings = list(store.iter_differences(comparison_type=ComparisonType.FINANCIAL))
    finally:
        store.close()

    assert len(findings) == 1
    assert findings[0].message == "Decimal precision differs"
    assert findings[0].value_a == 1
    assert findings[0].value_b == 2


def test_xml_element_order_is_strict_by_default_and_optional_to_ignore() -> None:
    record_a = XmlRecord(
        index=1,
        element=XmlElement(
            "record",
            (),
            None,
            (XmlElement("first", (), "1", (), None), XmlElement("second", (), "2", (), None)),
            None,
        ),
    )
    record_b = XmlRecord(
        index=1,
        element=XmlElement(
            "record",
            (),
            None,
            (XmlElement("second", (), "2", (), None), XmlElement("first", (), "1", (), None)),
            None,
        ),
    )

    assert list(XMLSchemaComparator().compare([record_a], [record_b]))
    assert (
        list(
            XMLSchemaComparator().compare(
                [record_a], [record_b], config=ComparisonConfig(strictness=Strictness.LENIENT)
            )
        )
        == []
    )


def test_raw_comparison_remains_byte_exact_even_when_semantic_rules_are_lenient() -> None:
    config = ComparisonConfig(
        strictness=Strictness.LENIENT,
        ignore_fields=["timestamp"],
        whitespace=WhitespaceMode.TRIM,
    )
    result = RawComparator().compare(io.BytesIO(b"timestamp=old\n"), io.BytesIO(b"timestamp=new\n"))
    findings = list(result)

    assert config.ignore_fields
    assert result.difference_count == len(findings)
    assert findings
