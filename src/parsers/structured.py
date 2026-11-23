"""Streaming JSON, YAML, fixed-width, and plain-text parsers."""

from __future__ import annotations

import copy
import json
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO

import yaml

from parsers.base import Parser
from parsers.delimited import ParseWarning
from parsers.detection import DEFAULT_SAMPLE_SIZE_BYTES
from parsers.text_stream import detach_text_stream, open_detected_text

_TEXT_READ_SIZE = 64 * 1024


class _TextScalarSafeLoader(yaml.SafeLoader):
    """Keep implicitly typed YAML scalars as strings while using safe tags."""


_TextScalarSafeLoader.yaml_implicit_resolvers = copy.deepcopy(
    yaml.SafeLoader.yaml_implicit_resolvers
)
_TextScalarSafeLoader.yaml_implicit_resolvers.clear()


class StructuredParseError(ValueError):
    """A structured or plain-text source could not be parsed."""


class JSONNumber(str):
    """A JSON number token retaining its exact lexical spelling and numeric type."""


@dataclass(frozen=True)
class StructuredRecord:
    """One parsed JSON or YAML document/value."""

    index: int
    value: Any


@dataclass(frozen=True)
class FixedWidthField:
    """A named fixed-width field, measured in decoded characters."""

    name: str
    width: int


@dataclass(frozen=True)
class FixedWidthRecord:
    """One fixed-width input row."""

    index: int
    values: Mapping[str, str]
    extra_text: str = ""
    warnings: tuple[ParseWarning, ...] = ()


@dataclass(frozen=True)
class TextRecord:
    """One physical text line, including its original line terminator."""

    index: int
    text: str


class _JSONCharacterReader:
    """Bounded character reader that retains at most one JSON value at a time."""

    def __init__(self, stream, chunk_size: int = _TEXT_READ_SIZE) -> None:
        self._stream = stream
        self._chunk_size = chunk_size
        self._buffer = ""
        self._position = 0
        self._eof = False

    def peek(self) -> str:
        self._fill()
        return self._buffer[self._position : self._position + 1]

    def read(self) -> str:
        character = self.peek()
        if character:
            self._position += 1
        return character

    def skip_whitespace(self) -> None:
        while self.peek() and self.peek().isspace():
            self.read()

    def read_value_starting(self, first: str) -> str:
        if first in "[{":
            stack = ["]" if first == "[" else "}"]
            parts = [first]
            in_string = False
            escaped = False
            while stack:
                character = self.read()
                if not character:
                    raise StructuredParseError("Unexpected end of input inside a JSON value")
                parts.append(character)
                if in_string:
                    if escaped:
                        escaped = False
                    elif character == "\\":
                        escaped = True
                    elif character == '"':
                        in_string = False
                elif character == '"':
                    in_string = True
                elif character == "[":
                    stack.append("]")
                elif character == "{":
                    stack.append("}")
                elif character in "]}":
                    if character != stack[-1]:
                        raise StructuredParseError("Mismatched delimiters in JSON value")
                    stack.pop()
            return "".join(parts)
        if first == '"':
            parts = [first]
            self._read_string_tail(parts)
            return "".join(parts)
        parts = [first]
        while (character := self.peek()) and not character.isspace():
            parts.append(self.read())
        return "".join(parts)

    def read_value(self) -> str:
        self.skip_whitespace()
        first = self.read()
        if not first:
            raise StructuredParseError("Expected a JSON value, found end of input")
        if first not in "[{":
            parts = [first]
            while (character := self.peek()) and not character.isspace() and character not in ",]":
                parts.append(self.read())
            return "".join(parts)
        return self.read_value_starting(first)

    def _read_string_tail(self, parts: list[str]) -> None:
        escaped = False
        while True:
            character = self.read()
            if not character:
                raise StructuredParseError("Unexpected end of input inside a JSON string")
            parts.append(character)
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == '"':
                return

    def _fill(self) -> None:
        if self._position < len(self._buffer) or self._eof:
            return
        self._buffer = self._stream.read(self._chunk_size)
        self._position = 0
        self._eof = not self._buffer


class JSONParser(Parser[StructuredRecord]):
    """Incrementally parse a JSON array, document, or JSON Lines stream."""

    def __init__(
        self,
        *,
        filename: str | Path | None = None,
        encoding_override: str | None = None,
        json_lines: bool = False,
        sample_size_bytes: int = DEFAULT_SAMPLE_SIZE_BYTES,
    ) -> None:
        self.filename = str(filename) if filename is not None else None
        self.encoding_override = encoding_override
        self.json_lines = json_lines
        self.sample_size_bytes = sample_size_bytes

    def parse(self, source: BinaryIO) -> Iterator[StructuredRecord]:
        """Yield one top-level array member, document, or JSONL line at a time."""
        try:
            text_stream, _ = open_detected_text(
                source,
                filename=self.filename,
                encoding_override=self.encoding_override,
                sample_size_bytes=self.sample_size_bytes,
            )
        except (TypeError, ValueError) as exc:
            raise StructuredParseError(str(exc)) from exc

        try:
            if self.json_lines:
                yield from self._parse_lines(text_stream)
            else:
                yield from self._parse_document(text_stream)
        except UnicodeDecodeError as exc:
            raise StructuredParseError(
                f"Could not decode {self.filename or 'JSON source'} using {text_stream.encoding!r}"
            ) from exc
        finally:
            detach_text_stream(text_stream)

    def _parse_document(self, text_stream) -> Iterator[StructuredRecord]:
        reader = _JSONCharacterReader(text_stream)
        reader.skip_whitespace()
        first = reader.read()
        if first == "[":
            yield from self._parse_array(reader)
            return
        if not first:
            raise StructuredParseError("JSON source is empty")
        value_text = _read_started_value(reader, first)
        try:
            value = _json_loads(value_text)
        except json.JSONDecodeError as exc:
            raise _json_error(self.filename, exc) from exc
        reader.skip_whitespace()
        if reader.peek():
            raise StructuredParseError(
                f"Unexpected trailing data in {self.filename or 'JSON source'}"
            )
        yield StructuredRecord(index=1, value=value)

    def _parse_array(self, reader: _JSONCharacterReader) -> Iterator[StructuredRecord]:
        index = 0
        reader.skip_whitespace()
        if reader.peek() == "]":
            reader.read()
            reader.skip_whitespace()
            if reader.peek():
                raise StructuredParseError(
                    f"Unexpected trailing data in {self.filename or 'JSON source'}"
                )
            return
        while True:
            try:
                value_text = reader.read_value()
                value = _json_loads(value_text)
            except json.JSONDecodeError as exc:
                raise _json_error(self.filename, exc) from exc
            except StructuredParseError as exc:
                raise StructuredParseError(
                    f"Malformed JSON array in {self.filename or 'source'}: {exc}"
                ) from exc
            index += 1
            yield StructuredRecord(index=index, value=value)
            reader.skip_whitespace()
            separator = reader.read()
            if separator == "]":
                break
            if separator != ",":
                raise StructuredParseError(
                    f"Expected ',' or ']' after JSON array item in {self.filename or 'source'}"
                )
            reader.skip_whitespace()
            if reader.peek() == "]":
                raise StructuredParseError("JSON arrays must not have a trailing comma")

        reader.skip_whitespace()
        if reader.peek():
            raise StructuredParseError(
                f"Unexpected trailing data in {self.filename or 'JSON source'}"
            )

    def _parse_lines(self, text_stream) -> Iterator[StructuredRecord]:
        index = 0
        for line_number, line in enumerate(text_stream, start=1):
            if not line.strip():
                continue
            try:
                value = _json_loads(line)
            except json.JSONDecodeError as exc:
                raise StructuredParseError(
                    f"Malformed JSON Lines input in {self.filename or 'source'} "
                    f"at line {line_number}: {exc.msg}"
                ) from exc
            index += 1
            yield StructuredRecord(index=index, value=value)


class YAMLParser(Parser[StructuredRecord]):
    """Safely parse a YAML stream one document at a time."""

    def __init__(
        self,
        *,
        filename: str | Path | None = None,
        encoding_override: str | None = None,
        sample_size_bytes: int = DEFAULT_SAMPLE_SIZE_BYTES,
    ) -> None:
        self.filename = str(filename) if filename is not None else None
        self.encoding_override = encoding_override
        self.sample_size_bytes = sample_size_bytes

    def parse(self, source: BinaryIO) -> Iterator[StructuredRecord]:
        """Yield each YAML document without reading the full source at once."""
        try:
            text_stream, _ = open_detected_text(
                source,
                filename=self.filename,
                encoding_override=self.encoding_override,
                sample_size_bytes=self.sample_size_bytes,
            )
        except (TypeError, ValueError) as exc:
            raise StructuredParseError(str(exc)) from exc

        try:
            documents = yaml.load_all(text_stream, Loader=_TextScalarSafeLoader)
            for index, value in enumerate(documents, start=1):
                yield StructuredRecord(index=index, value=value)
        except yaml.YAMLError as exc:
            mark = getattr(exc, "problem_mark", None)
            line = mark.line + 1 if mark is not None else "unknown"
            raise StructuredParseError(
                f"Malformed YAML in {self.filename or 'source'} at line {line}"
            ) from exc
        except UnicodeDecodeError as exc:
            raise StructuredParseError(
                f"Could not decode {self.filename or 'YAML source'} using {text_stream.encoding!r}"
            ) from exc
        finally:
            detach_text_stream(text_stream)


class FixedWidthParser(Parser[FixedWidthRecord]):
    """Parse fixed-width text using caller-provided field widths."""

    def __init__(
        self,
        field_widths: Mapping[str, int] | tuple[int, ...] | list[int],
        *,
        filename: str | Path | None = None,
        encoding_override: str | None = None,
        sample_size_bytes: int = DEFAULT_SAMPLE_SIZE_BYTES,
    ) -> None:
        self.filename = str(filename) if filename is not None else None
        self.encoding_override = encoding_override
        self.sample_size_bytes = sample_size_bytes
        if isinstance(field_widths, Mapping):
            fields = tuple(FixedWidthField(name, width) for name, width in field_widths.items())
        else:
            fields = tuple(
                FixedWidthField(f"column_{index}", width)
                for index, width in enumerate(field_widths, start=1)
            )
        if not fields or any(field.width < 1 for field in fields):
            raise ValueError("fixed-width field widths must be a non-empty sequence of positives")
        names = [field.name for field in fields]
        if len(set(names)) != len(names):
            raise ValueError("fixed-width field names must be unique")
        self.fields = fields

    def parse(self, source: BinaryIO) -> Iterator[FixedWidthRecord]:
        """Yield fixed-width lines as text slices with width warnings."""
        try:
            text_stream, _ = open_detected_text(
                source,
                filename=self.filename,
                encoding_override=self.encoding_override,
                sample_size_bytes=self.sample_size_bytes,
            )
        except (TypeError, ValueError) as exc:
            raise StructuredParseError(str(exc)) from exc
        try:
            total_width = sum(field.width for field in self.fields)
            for index, line in enumerate(text_stream, start=1):
                row = _remove_line_terminator(line)
                values: dict[str, str] = {}
                offset = 0
                for field in self.fields:
                    values[field.name] = row[offset : offset + field.width]
                    offset += field.width
                warnings: list[ParseWarning] = []
                extra_text = row[total_width:]
                if len(row) < total_width:
                    warnings.append(
                        ParseWarning(
                            code="too_few_characters",
                            message=f"Expected {total_width} characters but found {len(row)}",
                            row_number=index,
                        )
                    )
                elif len(row) > total_width:
                    warnings.append(
                        ParseWarning(
                            code="too_many_characters",
                            message=f"Expected {total_width} characters but found {len(row)}",
                            row_number=index,
                        )
                    )
                yield FixedWidthRecord(
                    index=index,
                    values=values,
                    extra_text=extra_text,
                    warnings=tuple(warnings),
                )
        except UnicodeDecodeError as exc:
            raise StructuredParseError(
                f"Could not decode {self.filename or 'fixed-width source'} "
                f"using {text_stream.encoding!r}"
            ) from exc
        finally:
            detach_text_stream(text_stream)


class PlainTextParser(Parser[TextRecord]):
    """Yield physical lines, preserving their content and original terminators."""

    def __init__(
        self,
        *,
        filename: str | Path | None = None,
        encoding_override: str | None = None,
        sample_size_bytes: int = DEFAULT_SAMPLE_SIZE_BYTES,
    ) -> None:
        self.filename = str(filename) if filename is not None else None
        self.encoding_override = encoding_override
        self.sample_size_bytes = sample_size_bytes

    def parse(self, source: BinaryIO) -> Iterator[TextRecord]:
        """Yield each physical line with whitespace and line endings intact."""
        try:
            text_stream, _ = open_detected_text(
                source,
                filename=self.filename,
                encoding_override=self.encoding_override,
                sample_size_bytes=self.sample_size_bytes,
            )
        except (TypeError, ValueError) as exc:
            raise StructuredParseError(str(exc)) from exc
        try:
            for index, line in enumerate(text_stream, start=1):
                yield TextRecord(index=index, text=line)
        except UnicodeDecodeError as exc:
            raise StructuredParseError(
                f"Could not decode {self.filename or 'text source'} using {text_stream.encoding!r}"
            ) from exc
        finally:
            detach_text_stream(text_stream)


def _read_started_value(reader: _JSONCharacterReader, first: str) -> str:
    return reader.read_value_starting(first)


def _json_error(filename: str | None, exc: json.JSONDecodeError) -> StructuredParseError:
    return StructuredParseError(
        f"Malformed JSON in {filename or 'source'} at line {exc.lineno}, "
        f"column {exc.colno}: {exc.msg}"
    )


def _json_loads(text: str) -> Any:
    return json.loads(text, parse_int=JSONNumber, parse_float=JSONNumber)


def _remove_line_terminator(line: str) -> str:
    if line.endswith("\r\n"):
        return line[:-2]
    if line.endswith(("\r", "\n")):
        return line[:-1]
    return line
