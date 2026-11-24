"""Validated comparison settings and configuration-file loading."""

from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from fnmatch import fnmatchcase
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator


class WhitespaceMode(StrEnum):
    """Whitespace handling applied during normalized comparisons."""

    PRESERVE = "preserve"
    TRIM = "trim"
    COLLAPSE = "collapse"


class Strictness(StrEnum):
    """How strictly format and schema mismatches should be treated."""

    STRICT = "strict"
    LENIENT = "lenient"


class FieldRuleOverride(BaseModel):
    """Optional rule overrides for one field name or glob pattern."""

    model_config = ConfigDict(extra="forbid")

    ignore: bool | None = None
    null_equals_empty: bool | None = None
    case_sensitive: bool | None = None
    whitespace: WhitespaceMode | None = None
    numeric_tolerance: Decimal | None = Field(default=None, ge=Decimal("0"))
    treat_equal_dates_as_same: bool | None = None


class ComparisonLevels(BaseModel):
    """Enable or disable individual comparison levels."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    raw: bool = True
    schema_comparison: bool = Field(default=True, alias="schema")
    metadata: bool = True
    records: bool = True
    fields: bool = True
    financial: bool = True
    reconciliation: bool = True


class ReconciliationRules(BaseModel):
    """Fields used for streaming financial reconciliation aggregates."""

    model_config = ConfigDict(extra="forbid")

    currency_field: str | None = Field(default=None, min_length=1)
    amount_field: str | None = Field(default=None, min_length=1)
    debit_field: str | None = Field(default=None, min_length=1)
    credit_field: str | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def require_currency_for_monetary_fields(self) -> ReconciliationRules:
        has_monetary_field = any((self.amount_field, self.debit_field, self.credit_field))
        if has_monetary_field and not self.currency_field:
            raise ValueError("currency_field is required when configuring monetary fields")
        return self


class FinancialRules(BaseModel):
    """Fields used for row-level financial checks."""

    model_config = ConfigDict(extra="forbid")

    currency_field: str | None = Field(default=None, min_length=1)
    amount_field: str | None = Field(default=None, min_length=1)
    check_decimal_precision: bool = True
    check_sign: bool = True


class ComparisonConfig(BaseModel):
    """Rules used to configure a deterministic file comparison."""

    model_config = ConfigDict(extra="forbid")

    max_memory_mb: int = Field(default=512, gt=0)
    duckdb_threads: int = Field(default=4, gt=0)
    max_total_differences: int = Field(default=1_000_000, ge=0)
    levels: ComparisonLevels = Field(default_factory=ComparisonLevels)
    record_key: list[str] = Field(default_factory=list)
    financial_rules: FinancialRules = Field(default_factory=FinancialRules)
    reconciliation_rules: ReconciliationRules = Field(default_factory=ReconciliationRules)
    trim_keys: bool = False
    key_case_sensitive: bool = True
    ignore_fields: list[str] = Field(default_factory=list)
    field_overrides: dict[str, FieldRuleOverride] = Field(default_factory=dict)
    null_equals_empty: bool = False
    numeric_tolerance: Decimal = Field(default=Decimal("0"), ge=Decimal("0"))
    case_sensitive: bool = True
    whitespace: WhitespaceMode = WhitespaceMode.PRESERVE
    strictness: Strictness = Strictness.STRICT
    ignore_element_order: bool = False
    date_formats: list[str] = Field(default_factory=lambda: ["%Y-%m-%d", "%d/%m/%Y"])
    treat_equal_dates_as_same: bool = False

    @model_validator(mode="before")
    @classmethod
    def apply_lenient_preset_defaults(cls, data: Any) -> Any:
        if not isinstance(data, dict) or data.get("strictness") != Strictness.LENIENT:
            return data
        data = dict(data)
        defaults = {
            "null_equals_empty": True,
            "case_sensitive": False,
            "whitespace": WhitespaceMode.TRIM,
            "ignore_element_order": True,
            "treat_equal_dates_as_same": True,
        }
        for key, value in defaults.items():
            data.setdefault(key, value)
        data.setdefault("numeric_tolerance", Decimal("0.01"))
        return data

    @model_validator(mode="after")
    def validate_date_formats(self) -> ComparisonConfig:
        for index, date_format in enumerate(self.date_formats):
            try:
                sample = datetime(2001, 2, 3).strftime(date_format)
                datetime.strptime(sample, date_format)
            except ValueError as exc:
                raise ValueError(f"date_formats.{index} is not a valid format string") from exc
        return self

    def field_override(self, field_name: str) -> FieldRuleOverride | None:
        """Return the most specific matching field override, if any."""
        exact = self.field_overrides.get(field_name)
        if exact is not None:
            return exact
        matches = [
            (pattern.count("*") + pattern.count("?"), pattern, override)
            for pattern, override in self.field_overrides.items()
            if any(character in pattern for character in "*?[") and fnmatchcase(field_name, pattern)
        ]
        return min(matches, default=(0, "", None))[2]


def load_comparison_config(path: str | Path) -> ComparisonConfig:
    """Load and validate a JSON or YAML comparison configuration file."""
    config_path = Path(path)
    suffix = config_path.suffix.lower()
    text = config_path.read_text(encoding="utf-8")

    if suffix == ".json":
        data: Any = json.loads(text)
    elif suffix in {".yaml", ".yml"}:
        data = yaml.safe_load(text)
    else:
        raise ValueError(f"Unsupported configuration file extension: {suffix or '(none)'}")

    try:
        return ComparisonConfig.model_validate(data)
    except ValidationError as exc:
        details = "; ".join(
            f"{'.'.join(str(part) for part in error['loc']) or '<root>'}: {error['msg']}"
            for error in exc.errors()
        )
        raise ValueError(f"Invalid comparison config: {details}") from exc
