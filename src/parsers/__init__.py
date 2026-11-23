"""Format detection and incremental input parsers."""

from parsers.base import Parser
from parsers.delimited import (
    CSVParser,
    DelimitedParseError,
    DelimitedRecord,
    DelimitedTextParser,
    ParseWarning,
    TSVParser,
)
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
from parsers.pdf import PDFParseError, PDFTextLine, PDFTextParser
from parsers.structured import (
    FixedWidthField,
    FixedWidthParser,
    FixedWidthRecord,
    JSONParser,
    PlainTextParser,
    StructuredParseError,
    StructuredRecord,
    TextRecord,
    YAMLParser,
)
from parsers.xlsx import XLSXCellValue, XLSXParseError, XLSXParser, XLSXRecord
from parsers.xml import XmlElement, XMLParseError, XMLParser, XmlRecord, iter_xml_records

__all__ = [
    "CSVParser",
    "DEFAULT_SAMPLE_SIZE_BYTES",
    "DelimitedParseError",
    "DelimitedRecord",
    "DelimitedTextParser",
    "DetectionSource",
    "EncodingSource",
    "FileDetection",
    "FileFormat",
    "FixedWidthField",
    "FixedWidthParser",
    "FixedWidthRecord",
    "JSONParser",
    "LineEnding",
    "LineEndingCounts",
    "ParseWarning",
    "Parser",
    "PDFParseError",
    "PDFTextLine",
    "PDFTextParser",
    "PlainTextParser",
    "StructuredParseError",
    "StructuredRecord",
    "TSVParser",
    "TextRecord",
    "YAMLParser",
    "XMLParseError",
    "XMLParser",
    "XLSXParseError",
    "XLSXParser",
    "XLSXRecord",
    "XLSXCellValue",
    "XmlElement",
    "XmlRecord",
    "detect_file",
    "iter_xml_records",
]
