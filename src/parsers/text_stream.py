"""Shared bounded-sample decoding for text-based streaming parsers."""

from __future__ import annotations

import io
from pathlib import Path
from typing import BinaryIO

from parsers.detection import (
    DEFAULT_SAMPLE_SIZE_BYTES,
    FileDetection,
    detect_file,
)


class _PrefixedReader(io.RawIOBase):
    """Replay the detection sample before continuing from the source stream."""

    def __init__(self, prefix: bytes, source: BinaryIO) -> None:
        super().__init__()
        self._prefix = memoryview(prefix)
        self._offset = 0
        self._source = source

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: bytearray | memoryview) -> int:
        if self.closed:
            raise ValueError("I/O operation on closed parser stream")
        view = memoryview(buffer)
        written = 0
        if self._offset < len(self._prefix):
            written = min(len(view), len(self._prefix) - self._offset)
            view[:written] = self._prefix[self._offset : self._offset + written]
            self._offset += written
        if written < len(view):
            chunk = self._source.read(len(view) - written)
            if not isinstance(chunk, bytes):
                raise TypeError("parser source streams must return bytes")
            view[written : written + len(chunk)] = chunk
            written += len(chunk)
        return written


def open_detected_text(
    source: BinaryIO,
    *,
    filename: str | Path | None,
    encoding_override: str | None,
    sample_size_bytes: int = DEFAULT_SAMPLE_SIZE_BYTES,
) -> tuple[io.TextIOWrapper, FileDetection]:
    """Open a sample-detected text wrapper without taking ownership of source."""
    if sample_size_bytes < 1:
        raise ValueError("sample_size_bytes must be greater than zero")
    sample = source.read(sample_size_bytes)
    if not isinstance(sample, bytes):
        raise TypeError("parser source streams must return bytes")
    detection = detect_file(sample, filename, sample_size_bytes=sample_size_bytes)
    encoding = encoding_override or detection.encoding
    if encoding is None:
        raise ValueError("Could not detect a text encoding; provide encoding_override")
    raw_reader = _PrefixedReader(sample, source)
    buffered = io.BufferedReader(raw_reader)
    try:
        return io.TextIOWrapper(buffered, encoding=encoding, newline=""), detection
    except (LookupError, UnicodeError) as exc:
        buffered.close()
        raise ValueError(f"Unsupported or invalid text encoding {encoding!r}") from exc


def detach_text_stream(text_stream: io.TextIOWrapper) -> None:
    """Detach wrappers while leaving the caller-owned input stream open."""
    try:
        buffered = text_stream.detach()
    except (AttributeError, ValueError):
        return
    try:
        raw_stream = buffered.detach()
    except (AttributeError, ValueError):
        return
    raw_stream.close()
