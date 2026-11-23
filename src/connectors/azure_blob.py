"""Streaming connector for Azure Blob Storage."""

from __future__ import annotations

import io
from collections.abc import Iterator
from datetime import datetime
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import unquote, urlsplit, urlunsplit

from azure.core.exceptions import AzureError
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

from connectors.base import OpenedSource, SourceConnector, SourceMetadata

_AZURE_BLOB_SUFFIXES = (
    ".blob.core.windows.net",
    ".blob.core.usgovcloudapi.net",
    ".blob.core.chinacloudapi.cn",
)


class AzureBlobSourceError(RuntimeError):
    """An Azure Blob source could not be opened or read."""


class AzureBlobConnector(SourceConnector):
    """Stream blobs using Azure's default identity credential chain."""

    def __init__(
        self,
        *,
        credential: Any | None = None,
        service_client: Any | None = None,
    ) -> None:
        self._credential = credential
        self._service_client = service_client

    def can_handle(self, uri: str) -> bool:
        """Return whether the URI identifies an Azure Blob source."""
        try:
            parsed = urlsplit(uri)
        except ValueError:
            return False
        if parsed.scheme.lower() == "azure":
            return True
        return parsed.scheme.lower() == "https" and _is_blob_host(parsed.hostname)

    def open(self, uri: str) -> OpenedSource:
        """Open a blob and return a bounded, streaming reader."""
        account_url, container, blob, safe_uri, name = self._parse_uri(uri)
        owns_client = self._service_client is None
        owns_credential = self._credential is None and owns_client
        credential = self._credential
        if owns_credential:
            try:
                credential = DefaultAzureCredential()
            except (AzureError, ValueError, OSError) as exc:
                raise AzureBlobSourceError(
                    f"Could not configure Azure credentials ({type(exc).__name__})"
                ) from None

        service_client = self._service_client
        if service_client is None:
            try:
                service_client = BlobServiceClient(account_url, credential=credential)
            except (AzureError, ValueError, OSError) as exc:
                _close_if_owned(credential, owns_credential)
                raise AzureBlobSourceError(
                    f"Could not create Azure Blob client ({type(exc).__name__})"
                ) from None

        downloader = None
        try:
            blob_client = service_client.get_blob_client(container=container, blob=blob)
            properties = blob_client.get_blob_properties()
            downloader = blob_client.download_blob(max_concurrency=1)
            metadata = SourceMetadata(
                uri=safe_uri,
                source_type="azure_blob",
                name=name,
                size_bytes=_optional_nonnegative_int(getattr(properties, "size", None)),
                modified_at=_aware_datetime(getattr(properties, "last_modified", None)),
                content_type=_content_type(properties),
                etag=_optional_string(getattr(properties, "etag", None)),
                version_id=_optional_string(getattr(properties, "version_id", None)),
            )
            chunks = iter(downloader.chunks())
        except (AzureError, OSError, ValueError) as exc:
            _close_if_owned(downloader, True)
            _close_if_owned(service_client, owns_client)
            _close_if_owned(credential, owns_credential)
            raise AzureBlobSourceError(
                f"Could not open Azure Blob source ({type(exc).__name__})"
            ) from None

        stream = _AzureBlobStream(
            chunks,
            downloader=downloader,
            service_client=service_client,
            credential=credential,
            close_service_client=owns_client,
            close_credential=owns_credential,
        )
        return OpenedSource(stream, metadata)

    @staticmethod
    def _parse_uri(uri: str) -> tuple[str, str, str, str, str]:
        return _parse_uri(uri)


class _AzureBlobStream(io.RawIOBase):
    """Adapt Azure's iterator of download chunks to a binary stream."""

    def __init__(
        self,
        chunks: Iterator[bytes],
        *,
        downloader: Any,
        service_client: Any,
        credential: Any,
        close_service_client: bool,
        close_credential: bool,
    ) -> None:
        super().__init__()
        self._chunks = chunks
        self._pending = b""
        self._downloader = downloader
        self._service_client = service_client
        self._credential = credential
        self._close_service_client = close_service_client
        self._close_credential = close_credential

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        if self.closed:
            raise ValueError("I/O operation on closed Azure Blob source")
        if size == 0:
            return b""
        if size < 0:
            chunks = [self._pending]
            self._pending = b""
            try:
                chunks.extend(self._chunks)
            except (AzureError, OSError) as exc:
                raise AzureBlobSourceError(
                    f"Azure Blob download failed ({type(exc).__name__})"
                ) from None
            return b"".join(chunks)

        while len(self._pending) < size:
            try:
                self._pending += next(self._chunks)
            except StopIteration:
                break
            except (AzureError, OSError) as exc:
                raise AzureBlobSourceError(
                    f"Azure Blob download failed ({type(exc).__name__})"
                ) from None

        result, self._pending = self._pending[:size], self._pending[size:]
        return result

    def close(self) -> None:
        if self.closed:
            return
        try:
            _close_if_owned(self._downloader, True)
        finally:
            try:
                _close_if_owned(self._service_client, self._close_service_client)
            finally:
                try:
                    _close_if_owned(self._credential, self._close_credential)
                finally:
                    super().close()


def _parse_uri(uri: str) -> tuple[str, str, str, str, str]:
    try:
        parsed = urlsplit(uri)
        port = parsed.port
    except ValueError:
        raise ValueError("Azure Blob source URI is invalid") from None

    if parsed.username is not None or parsed.password is not None or port is not None:
        raise ValueError("Azure Blob source URI must not include user information or a port")
    if parsed.query or parsed.fragment:
        raise ValueError("Azure Blob source URI must not include a query or fragment")

    if parsed.scheme.lower() == "azure":
        account = parsed.hostname
        if not account:
            raise ValueError("Azure URI must include an account name")
        account_url = f"https://{account}.blob.core.windows.net"
        path = parsed.path.lstrip("/")
        safe_uri = urlunsplit(("azure", parsed.netloc, parsed.path, "", ""))
    elif parsed.scheme.lower() == "https" and _is_blob_host(parsed.hostname):
        account_url = f"https://{parsed.netloc}"
        account = parsed.hostname
        path = parsed.path.lstrip("/")
        safe_uri = urlunsplit(("https", parsed.netloc, parsed.path, "", ""))
    else:
        raise ValueError("Azure Blob source must use azure:// or an Azure Blob HTTPS URL")

    if account is None:
        raise ValueError("Azure Blob source URI must include an account")
    container, separator, blob = unquote(path).partition("/")
    if not separator or not container or not blob:
        raise ValueError("Azure Blob source URI must include a container and blob name")
    name = PurePosixPath(blob).name
    if not name:
        raise ValueError("Azure Blob source URI must include a blob file name")
    return account_url, container, blob, safe_uri, name


def _is_blob_host(hostname: str | None) -> bool:
    if hostname is None:
        return False
    return any(hostname.lower().endswith(suffix) for suffix in _AZURE_BLOB_SUFFIXES)


def _optional_nonnegative_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return value
    raise ValueError("Azure Blob size must be a nonnegative integer")


def _optional_string(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _aware_datetime(value: object) -> datetime | None:
    if not isinstance(value, datetime) or value.utcoffset() is None:
        return None
    return value


def _content_type(properties: Any) -> str | None:
    settings = getattr(properties, "content_settings", None)
    return _optional_string(getattr(settings, "content_type", None))


def _close_if_owned(resource: Any, owned: bool) -> None:
    if owned and resource is not None:
        close = getattr(resource, "close", None)
        if callable(close):
            close()
