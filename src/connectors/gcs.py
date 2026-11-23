"""Streaming connector for Google Cloud Storage."""

from __future__ import annotations

import io
from datetime import datetime
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import unquote, urlsplit

from google.api_core.exceptions import GoogleAPIError
from google.cloud import storage

from connectors.base import OpenedSource, SourceConnector, SourceMetadata


class GCSSourceError(RuntimeError):
    """A Google Cloud Storage source could not be opened or read."""


class GCSConnector(SourceConnector):
    """Stream objects using Google Application Default Credentials."""

    def __init__(
        self,
        *,
        client: Any | None = None,
        chunk_size: int = 4 * 1024 * 1024,
    ) -> None:
        if chunk_size < 256 * 1024:
            raise ValueError("chunk_size must be at least 256 KiB for GCS streaming")
        self._client = client
        self._chunk_size = chunk_size

    def can_handle(self, uri: str) -> bool:
        """Return whether the URI uses the gs scheme."""
        try:
            return urlsplit(uri).scheme.lower() == "gs"
        except ValueError:
            return False

    def open(self, uri: str) -> OpenedSource:
        """Open a GCS object reader and attach object metadata."""
        bucket_name, object_name = self._parse_uri(uri)
        owns_client = self._client is None
        client = self._client
        if client is None:
            try:
                client = storage.Client()
            except (GoogleAPIError, OSError, ValueError) as exc:
                raise GCSSourceError(
                    f"Could not configure Google Cloud credentials ({type(exc).__name__})"
                ) from None

        reader = None
        try:
            blob = client.bucket(bucket_name).blob(object_name)
            blob.reload()
            reader = blob.open("rb", chunk_size=self._chunk_size)
            metadata = SourceMetadata(
                uri=f"gs://{bucket_name}/{object_name}",
                source_type="gcs",
                name=PurePosixPath(object_name).name or object_name,
                size_bytes=_optional_nonnegative_int(blob.size),
                modified_at=_aware_datetime(blob.updated),
                content_type=_optional_string(blob.content_type),
                etag=_optional_string(blob.etag),
                version_id=_optional_version(blob.generation),
            )
        except (GoogleAPIError, OSError, ValueError) as exc:
            _close_if_owned(reader, True)
            _close_if_owned(client, owns_client)
            raise GCSSourceError(
                f"Could not open Google Cloud Storage source ({type(exc).__name__})"
            ) from None

        return OpenedSource(
            _GCSStream(reader, client=client, close_client=owns_client),
            metadata,
        )

    @staticmethod
    def _parse_uri(uri: str) -> tuple[str, str]:
        try:
            parsed = urlsplit(uri)
            port = parsed.port
        except ValueError:
            raise ValueError("GCS source URI is invalid") from None

        if parsed.scheme.lower() != "gs" or not parsed.netloc:
            raise ValueError("GCS source URI must use gs://bucket/object")
        if parsed.username is not None or parsed.password is not None or port is not None:
            raise ValueError("GCS source URI must not include user information or a port")
        if parsed.query or parsed.fragment:
            raise ValueError("GCS source URI must not include a query or fragment")

        object_name = unquote(parsed.path.lstrip("/"))
        if not object_name:
            raise ValueError("GCS source URI must include an object name")
        return parsed.netloc, object_name


class _GCSStream(io.RawIOBase):
    """Wrap a GCS BlobReader and close its internally-created client."""

    def __init__(self, reader: Any, *, client: Any, close_client: bool) -> None:
        super().__init__()
        self._reader = reader
        self._client = client
        self._close_client = close_client

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        if self.closed:
            raise ValueError("I/O operation on closed GCS source")
        try:
            return self._reader.read(size)
        except (GoogleAPIError, OSError) as exc:
            raise GCSSourceError(
                f"Google Cloud Storage download failed ({type(exc).__name__})"
            ) from None

    def close(self) -> None:
        if self.closed:
            return
        try:
            self._reader.close()
        finally:
            try:
                _close_if_owned(self._client, self._close_client)
            finally:
                super().close()


def _optional_nonnegative_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return value
    raise ValueError("GCS object size must be a nonnegative integer")


def _optional_string(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _optional_version(value: object) -> str | None:
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return str(value)
    return _optional_string(value)


def _aware_datetime(value: object) -> datetime | None:
    if not isinstance(value, datetime) or value.utcoffset() is None:
        return None
    return value


def _close_if_owned(resource: Any, owned: bool) -> None:
    if owned and resource is not None:
        close = getattr(resource, "close", None)
        if callable(close):
            close()
