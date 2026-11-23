"""Deterministic file comparison engines."""

from comparators.fields import FieldComparator, FieldComparisonResult
from comparators.financial import FinancialComparator, FinancialComparisonResult
from comparators.metadata import MetadataComparator, MetadataComparison, MetadataStatus
from comparators.raw import (
    RawComparator,
    RawComparisonResult,
    RawComparisonStatus,
    RawDifference,
    RawDifferenceKind,
)
from comparators.reconciliation import (
    ReconciliationComparator,
    ReconciliationComparisonResult,
)
from comparators.records import (
    RecordComparator,
    RecordComparisonResult,
    parser_record_to_text_mapping,
)
from comparators.xml_schema import XMLSchemaComparator, XMLSchemaError, validate_xml_schema

__all__ = [
    "MetadataComparator",
    "MetadataComparison",
    "MetadataStatus",
    "FieldComparator",
    "FieldComparisonResult",
    "FinancialComparator",
    "FinancialComparisonResult",
    "RawComparator",
    "RawComparisonResult",
    "RawComparisonStatus",
    "RawDifference",
    "RawDifferenceKind",
    "RecordComparator",
    "RecordComparisonResult",
    "ReconciliationComparator",
    "ReconciliationComparisonResult",
    "XMLSchemaComparator",
    "XMLSchemaError",
    "validate_xml_schema",
    "parser_record_to_text_mapping",
]
