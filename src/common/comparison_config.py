"""Validated comparison settings and configuration-file loading."""

import json
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field


class WhitespaceMode(StrEnum):
    """Whitespace handling applied during normalized comparisons."""

    PRESERVE = "preserve"
    TRIM = "trim"
    COLLAPSE = "collapse"


class Strictness(StrEnum):
    """How strictly format and schema mismatches should be treated."""

    STRICT = "strict"
    LENIENT = "lenient"


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


class ComparisonConfig(BaseModel):
    """Rules used to configure a deterministic file comparison."""

    model_config = ConfigDict(extra="forbid")

    max_memory_mb: int = Field(default=512, gt=0)
    max_total_differences: int = Field(default=1_000_000, ge=0)
    levels: ComparisonLevels = Field(default_factory=ComparisonLevels)
    record_key: list[str] = Field(default_factory=list)
    ignore_fields: list[str] = Field(default_factory=list)
    numeric_tolerance: Decimal = Field(default=Decimal("0"), ge=Decimal("0"))
    case_sensitive: bool = True
    whitespace: WhitespaceMode = WhitespaceMode.PRESERVE
    strictness: Strictness = Strictness.STRICT


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

    return ComparisonConfig.model_validate(data)
