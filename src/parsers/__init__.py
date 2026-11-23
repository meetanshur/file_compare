"""Format detection and incremental input parsers."""

from parsers.detection import (
    DEFAULT_SAMPLE_SIZE_BYTES,
    DetectionSource,
    EncodingSource,
    FileDetection,
    FileFormat,
    LineEnding,
    LineEndingCounts,
    detect_file,
)
from parsers.xml import XmlElement, XMLParseError, XmlRecord, iter_xml_records

__all__ = [
    "DEFAULT_SAMPLE_SIZE_BYTES",
    "DetectionSource",
    "EncodingSource",
    "FileDetection",
    "FileFormat",
    "LineEnding",
    "LineEndingCounts",
    "XMLParseError",
    "XmlElement",
    "XmlRecord",
    "detect_file",
    "iter_xml_records",
]
