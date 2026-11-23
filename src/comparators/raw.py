"""Streaming byte- and character-level comparison."""

from __future__ import annotations

import codecs
from collections import deque
from collections.abc import Iterator
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import BinaryIO

from parsers.detection import DEFAULT_SAMPLE_SIZE_BYTES, detect_file


class RawDifferenceKind(StrEnum):
    """Representation in which a raw difference was observed."""

    BYTE = "byte"
    CHARACTER = "character"


@dataclass(frozen=True)
class RawDifference:
    """One byte or decoded-character mismatch with positions in both inputs."""

    kind: RawDifferenceKind
    offset_a: int | None
    offset_b: int | None
    line_a: int | None
    column_a: int | None
    line_b: int | None
    column_b: int | None
    value_a: int | str | None
    value_b: int | str | None


@dataclass(frozen=True)
class _LocatedCharacter:
    value: str
    offset: int
    line: int
    column: int


class _PositionTracker:
    def __init__(self) -> None:
        self.offset = 0
        self.line = 1
        self.column = 1
        self._pending_carriage_return = False

    def advance(self, value: int | str) -> tuple[int, int, int]:
        if self._pending_carriage_return:
            if value == 10 or value == "\n":
                position = (self.offset, self.line, self.column)
                self.line += 1
                self.column = 1
                self._pending_carriage_return = False
                self.offset += 1
                return position
            self.line += 1
            self.column = 1
            self._pending_carriage_return = False

        position = (self.offset, self.line, self.column)
        if value == 13 or value == "\r":
            self._pending_carriage_return = True
        elif value == 10 or value == "\n":
            self.line += 1
            self.column = 1
        else:
            self.column += 1
        self.offset += 1
        return position


class RawComparator:
    """Compare two binary streams without retaining their full contents."""

    def __init__(
        self,
        *,
        encoding_a: str | None = None,
        encoding_b: str | None = None,
        filename_a: str | Path | None = None,
        filename_b: str | Path | None = None,
        chunk_size: int = 4 * 1024 * 1024,
        sample_size_bytes: int = DEFAULT_SAMPLE_SIZE_BYTES,
    ) -> None:
        if chunk_size < 1:
            raise ValueError("chunk_size must be greater than zero")
        if sample_size_bytes < 1:
            raise ValueError("sample_size_bytes must be greater than zero")
        self.encoding_a = encoding_a
        self.encoding_b = encoding_b
        self.filename_a = str(filename_a) if filename_a is not None else None
        self.filename_b = str(filename_b) if filename_b is not None else None
        self.chunk_size = chunk_size
        self.sample_size_bytes = sample_size_bytes

    def compare(self, source_a: BinaryIO, source_b: BinaryIO) -> Iterator[RawDifference]:
        """Yield every byte and decoded-character difference in input order."""
        prefix_a = _read_prefix(source_a, self.sample_size_bytes, self.chunk_size)
        prefix_b = _read_prefix(source_b, self.sample_size_bytes, self.chunk_size)
        decoder_a = _make_decoder(_choose_encoding(self.encoding_a, prefix_a, self.filename_a))
        decoder_b = _make_decoder(_choose_encoding(self.encoding_b, prefix_b, self.filename_b))
        chunks_a = iter(_chunks(source_a, prefix_a, self.chunk_size))
        chunks_b = iter(_chunks(source_b, prefix_b, self.chunk_size))
        byte_positions_a = _PositionTracker()
        byte_positions_b = _PositionTracker()
        character_positions_a = _PositionTracker()
        character_positions_b = _PositionTracker()
        characters_a: deque[_LocatedCharacter] = deque()
        characters_b: deque[_LocatedCharacter] = deque()
        character_offset_a = 0
        character_offset_b = 0
        ended_a = False
        ended_b = False

        while not (ended_a and ended_b):
            chunk_a, ended_a = _next_chunk(chunks_a, ended_a)
            chunk_b, ended_b = _next_chunk(chunks_b, ended_b)

            for index in range(max(len(chunk_a), len(chunk_b))):
                value_a = chunk_a[index] if index < len(chunk_a) else None
                value_b = chunk_b[index] if index < len(chunk_b) else None
                location_a = byte_positions_a.advance(value_a) if value_a is not None else None
                location_b = byte_positions_b.advance(value_b) if value_b is not None else None
                if value_a != value_b:
                    yield RawDifference(
                        kind=RawDifferenceKind.BYTE,
                        offset_a=None if location_a is None else location_a[0],
                        offset_b=None if location_b is None else location_b[0],
                        line_a=None if location_a is None else location_a[1],
                        column_a=None if location_a is None else location_a[2],
                        line_b=None if location_b is None else location_b[1],
                        column_b=None if location_b is None else location_b[2],
                        value_a=value_a,
                        value_b=value_b,
                    )

            text_a = decoder_a.decode(chunk_a, final=ended_a)
            text_b = decoder_b.decode(chunk_b, final=ended_b)
            for character in text_a:
                location = character_positions_a.advance(character)
                characters_a.append(
                    _LocatedCharacter(character, character_offset_a, location[1], location[2])
                )
                character_offset_a += 1
            for character in text_b:
                location = character_positions_b.advance(character)
                characters_b.append(
                    _LocatedCharacter(character, character_offset_b, location[1], location[2])
                )
                character_offset_b += 1

            while characters_a and characters_b:
                character_a = characters_a.popleft()
                character_b = characters_b.popleft()
                if character_a.value != character_b.value:
                    yield _character_difference(character_a, character_b)

            if ended_a:
                while characters_b:
                    yield _character_difference(None, characters_b.popleft())
            if ended_b:
                while characters_a:
                    yield _character_difference(characters_a.popleft(), None)


def _character_difference(
    character_a: _LocatedCharacter | None,
    character_b: _LocatedCharacter | None,
) -> RawDifference:
    return RawDifference(
        kind=RawDifferenceKind.CHARACTER,
        offset_a=None if character_a is None else character_a.offset,
        offset_b=None if character_b is None else character_b.offset,
        line_a=None if character_a is None else character_a.line,
        column_a=None if character_a is None else character_a.column,
        line_b=None if character_b is None else character_b.line,
        column_b=None if character_b is None else character_b.column,
        value_a=None if character_a is None else character_a.value,
        value_b=None if character_b is None else character_b.value,
    )


def _read_prefix(source: BinaryIO, size: int, chunk_size: int) -> bytes:
    prefix = bytearray()
    while len(prefix) < size:
        chunk = _read_bytes(source, min(chunk_size, size - len(prefix)))
        if not chunk:
            break
        prefix.extend(chunk)
    return bytes(prefix)


def _chunks(source: BinaryIO, prefix: bytes, chunk_size: int) -> Iterator[bytes]:
    if prefix:
        yield prefix
    while True:
        chunk = _read_filled_chunk(source, chunk_size)
        if not chunk:
            return
        yield chunk


def _read_filled_chunk(source: BinaryIO, size: int) -> bytes:
    chunk = bytearray()
    while len(chunk) < size:
        part = _read_bytes(source, size - len(chunk))
        if not part:
            break
        chunk.extend(part)
    return bytes(chunk)


def _read_bytes(source: BinaryIO, size: int) -> bytes:
    chunk = source.read(size)
    if not isinstance(chunk, bytes):
        raise TypeError("comparison sources must return bytes")
    return chunk


def _next_chunk(chunks: Iterator[bytes], already_ended: bool) -> tuple[bytes, bool]:
    if already_ended:
        return b"", True
    try:
        return next(chunks), False
    except StopIteration:
        return b"", True


def _choose_encoding(
    override: str | None,
    sample: bytes,
    filename: str | None,
) -> str:
    if override is not None:
        try:
            return codecs.lookup(override).name
        except LookupError as exc:
            raise ValueError(f"Unknown text encoding {override!r}") from exc
    detection = detect_file(sample, filename, sample_size_bytes=len(sample) or 1)
    return detection.encoding or "latin-1"


def _make_decoder(encoding: str):
    try:
        return codecs.getincrementaldecoder(encoding)(errors="surrogateescape")
    except LookupError as exc:
        raise ValueError(f"Unknown text encoding {encoding!r}") from exc
