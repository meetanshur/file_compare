import json
from decimal import Decimal

import pytest
from pydantic import ValidationError

from common.comparison_config import (
    ComparisonConfig,
    Strictness,
    WhitespaceMode,
    load_comparison_config,
)
from common.comparison_result import (
    ComparisonResult,
    ComparisonSummary,
    ComparisonType,
    Difference,
    Severity,
)


def test_load_yaml_config(tmp_path) -> None:
    config_path = tmp_path / "comparison.yaml"
    config_path.write_text(
        'record_key: [id]\nnumeric_tolerance: "0.05"\nwhitespace: trim\nstrictness: lenient\n',
        encoding="utf-8",
    )

    config = load_comparison_config(config_path)

    assert config.record_key == ["id"]
    assert config.numeric_tolerance == Decimal("0.05")
    assert config.whitespace is WhitespaceMode.TRIM
    assert config.strictness is Strictness.LENIENT


def test_load_json_config(tmp_path) -> None:
    config_path = tmp_path / "comparison.json"
    config_path.write_text(
        json.dumps({"levels": {"raw": False, "schema": False}, "ignore_fields": ["timestamp"]}),
        encoding="utf-8",
    )

    config = load_comparison_config(config_path)

    assert config.levels.raw is False
    assert config.levels.schema_comparison is False
    assert config.levels.records is True
    assert config.ignore_fields == ["timestamp"]


def test_config_rejects_negative_tolerance_and_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        ComparisonConfig(numeric_tolerance=Decimal("-0.01"))
    with pytest.raises(ValidationError):
        ComparisonConfig(unexpected_option=True)


def test_result_models_represent_findings_and_aggregate_counts() -> None:
    finding = Difference(
        comparison_type=ComparisonType.UPDATE,
        location="row 3.amount",
        message="Values differ",
        severity=Severity.ERROR,
        value_a=Decimal("10.00"),
        value_b=Decimal("11.00"),
    )
    result = ComparisonResult(
        summary=ComparisonSummary(total_differences=1, updated=1),
    )

    assert finding.value_a == Decimal("10.00")
    assert result.summary.total_differences == 1
    assert result.comparison_id
