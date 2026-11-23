"""Shared contracts for incremental source parsers."""

from __future__ import annotations

from collections.abc import Iterator
from typing import BinaryIO, Protocol, TypeVar, runtime_checkable

RecordT = TypeVar("RecordT", covariant=True)


@runtime_checkable
class Parser(Protocol[RecordT]):
    """A parser that consumes a binary source incrementally and yields records."""

    def parse(self, source: BinaryIO) -> Iterator[RecordT]:
        """Yield parsed records without loading the complete source."""
