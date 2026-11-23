"""Shared contracts for streaming file-source connectors."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import BinaryIO


@dataclass(frozen=True)
class SourceMetadata:
    """Metadata a connector can provide about a source file."""

    uri: str
    source_type: str
    name: str
    size_bytes: int | None = None
    modified_at: datetime | None = None
    content_type: str | None = None
    etag: str | None = None
    version_id: str | None = None
    checksums: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.uri:
            raise ValueError("uri must not be empty")
        if not self.source_type:
            raise ValueError("source_type must not be empty")
        if not self.name:
            raise ValueError("name must not be empty")
        if self.size_bytes is not None and self.size_bytes < 0:
            raise ValueError("size_bytes must be zero or greater")
        if self.modified_at is not None and self.modified_at.utcoffset() is None:
            raise ValueError("modified_at must include a timezone")


class OpenedSource:
    """An opened binary source stream with its metadata and lifecycle."""

    def __init__(self, stream: BinaryIO, metadata: SourceMetadata) -> None:
        self.stream = stream
        self.metadata = metadata
        self._closed = False

    def __enter__(self) -> OpenedSource:
        if self._closed:
            raise ValueError("source stream is closed")
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        self.close()

    def close(self) -> None:
        """Close the underlying stream once."""
        if not self._closed:
            self.stream.close()
            self._closed = True

    def read_chunks(self, chunk_size: int) -> Iterator[bytes]:
        """Yield bounded chunks until EOF without loading the complete source."""
        if chunk_size < 1:
            raise ValueError("chunk_size must be greater than zero")
        if self._closed:
            raise ValueError("source stream is closed")

        while True:
            chunk = self.stream.read(chunk_size)
            if not isinstance(chunk, bytes):
                raise TypeError("source streams must return bytes")
            if not chunk:
                return
            yield chunk


class SourceConnector(ABC):
    """Base contract implemented by local and remote source connectors."""

    @abstractmethod
    def can_handle(self, uri: str) -> bool:
        """Return whether this connector recognizes the supplied source URI."""

    @abstractmethod
    def open(self, uri: str) -> OpenedSource:
        """Open a source and return a context-managed binary stream."""
