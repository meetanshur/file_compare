"""Typed comparison findings and aggregate results."""

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import UUID, uuid4

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field


class ComparisonType(StrEnum):
    """Comparison category that produced a finding."""

    RAW = "raw"
    SCHEMA = "schema"
    METADATA = "metadata"
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    FIELD = "field"
    FINANCIAL = "financial"
    RECONCILIATION = "reconciliation"


class Severity(StrEnum):
    """Impact level assigned to a comparison finding."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class Difference(BaseModel):
    """One deterministic comparison finding."""

    model_config = ConfigDict(extra="forbid")

    comparison_type: ComparisonType
    location: str
    message: str
    severity: Severity = Severity.WARNING
    value_a: Any = None
    value_b: Any = None


class ComparisonSummary(BaseModel):
    """Aggregate counts that can be retained without loading all findings."""

    model_config = ConfigDict(extra="forbid")

    total_differences: int = Field(default=0, ge=0)
    records_file_a: int = Field(default=0, ge=0)
    records_file_b: int = Field(default=0, ge=0)
    inserted: int = Field(default=0, ge=0)
    updated: int = Field(default=0, ge=0)
    deleted: int = Field(default=0, ge=0)


class ComparisonResult(BaseModel):
    """Top-level comparison result summary and run metadata."""

    model_config = ConfigDict(extra="forbid")

    comparison_id: UUID = Field(default_factory=uuid4)
    created_at: AwareDatetime = Field(default_factory=lambda: datetime.now(UTC))
    summary: ComparisonSummary = Field(default_factory=ComparisonSummary)
    warnings: list[str] = Field(default_factory=list)
