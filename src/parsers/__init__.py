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

__all__ = [
    "DEFAULT_SAMPLE_SIZE_BYTES",
    "DetectionSource",
    "EncodingSource",
    "FileDetection",
    "FileFormat",
    "LineEnding",
    "LineEndingCounts",
    "detect_file",
]
