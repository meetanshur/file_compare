"""Streaming connector for HTTP and HTTPS sources."""

from __future__ import annotations

import io
import re
from collections.abc import Callable
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import PurePosixPath
from urllib.error import HTTPError, URLError
from urllib.parse import SplitResult, unquote, urlsplit, urlunsplit
from urllib.request import Request, urlopen

from connectors.base import OpenedSource, SourceConnector, SourceMetadata

_CONTENT_RANGE = re.compile(r"bytes (\d+)-(\d+)/(\d+|\*)", re.IGNORECASE)


class HTTPSourceError(RuntimeError):
    """An HTTP source could not be opened or streamed."""


class HTTPConnector(SourceConnector):
    """Stream HTTP(S) sources and use byte ranges when the server supports them."""

    def __init__(
        self,
        *,
        range_chunk_size: int = 4 * 1024 * 1024,
        timeout: float = 30,
        opener: Callable[..., object] | None = None,
    ) -> None:
        if range_chunk_size < 1:
            raise ValueError("range_chunk_size must be greater than zero")
        if timeout <= 0:
            raise ValueError("timeout must be greater than zero")
        self._range_chunk_size = range_chunk_size
        self._timeout = timeout
        self._opener = opener or urlopen

    def can_handle(self, uri: str) -> bool:
        """Return whether the URI uses HTTP or HTTPS."""
        try:
            return urlsplit(uri).scheme.lower() in {"http", "https"}
        except ValueError:
            return False

    def open(self, uri: str) -> OpenedSource:
        """Open an HTTP(S) source, preferring bounded byte-range responses."""
        parsed = _validate_uri(uri)
        first_response = self._request(uri, start=0, end=self._range_chunk_size - 1)
        try:
            status = _response_status(first_response)
        except HTTPSourceError:
            first_response.close()
            raise

        if status == 200:
            metadata = _metadata(
                parsed,
                first_response,
                size_bytes=_content_length(first_response),
            )
            return OpenedSource(first_response, metadata)  # type: ignore[arg-type]
        if status != 206:
            first_response.close()
            raise HTTPSourceError(f"HTTP source returned HTTP {status}")

        try:
            range_start, range_end, total_size = _parse_content_range(first_response)
            if range_start != 0 or range_end >= self._range_chunk_size:
                raise HTTPSourceError("HTTP server returned an invalid byte range")
            metadata = _metadata(parsed, first_response, size_bytes=total_size)
        except (HTTPSourceError, ValueError):
            first_response.close()
            raise

        stream = _RangeStream(
            uri=uri,
            opener=self._opener,
            timeout=self._timeout,
            range_chunk_size=self._range_chunk_size,
            first_response=first_response,
            first_range_end=range_end,
            total_size=total_size,
        )
        return OpenedSource(stream, metadata)  # type: ignore[arg-type]

    def _request(self, uri: str, *, start: int, end: int) -> object:
        request = Request(uri, headers={"Range": f"bytes={start}-{end}"})
        try:
            return self._opener(request, timeout=self._timeout)
        except HTTPError as exc:
            exc.close()
            raise HTTPSourceError(f"HTTP source returned HTTP {exc.code}") from None
        except (URLError, TimeoutError, OSError) as exc:
            raise HTTPSourceError(f"HTTP source request failed ({type(exc).__name__})") from None


class _RangeStream(io.RawIOBase):
    """Read sequentially from bounded HTTP range responses."""

    def __init__(
        self,
        *,
        uri: str,
        opener: Callable[..., object],
        timeout: float,
        range_chunk_size: int,
        first_response: object,
        first_range_end: int,
        total_size: int | None,
    ) -> None:
        super().__init__()
        self._uri = uri
        self._opener = opener
        self._timeout = timeout
        self._range_chunk_size = range_chunk_size
        self._response = first_response
        self._offset = 0
        self._range_end = first_range_end + 1
        self._total_size = total_size

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        if self.closed:
            raise ValueError("I/O operation on closed HTTP source")
        if size == 0:
            return b""
        if size < 0:
            chunks = []
            while chunk := self.read(self._range_chunk_size):
                chunks.append(chunk)
            return b"".join(chunks)

        chunks: list[bytes] = []
        remaining = size
        while remaining:
            if self._total_size is not None and self._offset >= self._total_size:
                break
            if self._offset >= self._range_end:
                if not self._open_next_range():
                    break

            to_read = min(remaining, self._range_end - self._offset)
            chunk = self._response.read(to_read)
            if not chunk:
                if self._offset < self._range_end:
                    raise HTTPSourceError(
                        "HTTP range response ended before its declared byte range"
                    )
                continue
            chunks.append(chunk)
            self._offset += len(chunk)
            remaining -= len(chunk)

        return b"".join(chunks)

    def close(self) -> None:
        if not self.closed:
            self._response.close()
        super().close()

    def _open_next_range(self) -> bool:
        self._response.close()
        if self._total_size is not None and self._offset >= self._total_size:
            return False

        start = self._offset
        end = start + self._range_chunk_size - 1
        if self._total_size is not None:
            end = min(end, self._total_size - 1)

        request = Request(self._uri, headers={"Range": f"bytes={start}-{end}"})
        try:
            response = self._opener(request, timeout=self._timeout)
        except HTTPError as exc:
            exc.close()
            raise HTTPSourceError(f"HTTP range request returned HTTP {exc.code}") from None
        except (URLError, TimeoutError, OSError) as exc:
            raise HTTPSourceError(f"HTTP range request failed ({type(exc).__name__})") from None

        try:
            status = _response_status(response)
        except HTTPSourceError:
            response.close()
            raise
        if status != 206:
            response.close()
            raise HTTPSourceError(
                f"HTTP range request returned HTTP {status}; server stopped honoring byte ranges"
            )

        try:
            range_start, range_end, total_size = _parse_content_range(response)
            if range_start != start or range_end > end:
                raise HTTPSourceError("HTTP server returned an invalid byte range")
            if (
                self._total_size is not None
                and total_size is not None
                and total_size != self._total_size
            ):
                raise HTTPSourceError("HTTP source size changed during range streaming")
            self._total_size = self._total_size if self._total_size is not None else total_size
            self._response = response
            self._range_end = range_end + 1
        except (HTTPSourceError, ValueError):
            response.close()
            raise
        return True


def _validate_uri(uri: str) -> SplitResult:
    try:
        parsed = urlsplit(uri)
        port = parsed.port
    except ValueError:
        raise ValueError("HTTP source URI is invalid") from None
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.hostname:
        raise ValueError("HTTP source URI must be an absolute HTTP or HTTPS URL")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("HTTP source URI must not contain user information")
    if port is not None and not 1 <= port <= 65535:
        raise ValueError("HTTP source URI port is invalid")
    return parsed


def _metadata(
    parsed: SplitResult,
    response: object,
    *,
    size_bytes: int | None,
) -> SourceMetadata:
    headers = response.headers
    name = PurePosixPath(unquote(parsed.path)).name or parsed.hostname or "http-source"
    modified_at = _last_modified(headers.get("Last-Modified"))
    return SourceMetadata(
        uri=urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", "")),
        source_type="http",
        name=name,
        size_bytes=size_bytes,
        modified_at=modified_at,
        content_type=headers.get("Content-Type"),
        etag=headers.get("ETag"),
    )


def _response_status(response: object) -> int:
    status = getattr(response, "status", None)
    if isinstance(status, int):
        return status
    getcode = getattr(response, "getcode", None)
    if callable(getcode):
        code = getcode()
        if isinstance(code, int):
            return code
    raise HTTPSourceError("HTTP response did not include a status code")


def _content_length(response: object) -> int | None:
    value = response.headers.get("Content-Length")
    if value is None:
        return None
    try:
        size = int(value)
    except (TypeError, ValueError):
        return None
    return size if size >= 0 else None


def _parse_content_range(response: object) -> tuple[int, int, int | None]:
    value = response.headers.get("Content-Range", "")
    match = _CONTENT_RANGE.fullmatch(value.strip())
    if match is None:
        raise HTTPSourceError("HTTP 206 response did not include a valid Content-Range")
    start, end = int(match.group(1)), int(match.group(2))
    if end < start:
        raise HTTPSourceError("HTTP server returned an invalid byte range")
    total = None if match.group(3) == "*" else int(match.group(3))
    if total is not None and (total <= end or total < 0):
        raise HTTPSourceError("HTTP server returned an invalid total size")
    return start, end, total


def _last_modified(value: str | None) -> datetime | None:
    if value is None:
        return None
    try:
        modified_at = parsedate_to_datetime(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if modified_at.utcoffset() is None:
        return modified_at.replace(tzinfo=UTC)
    return modified_at
