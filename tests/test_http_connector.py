from __future__ import annotations

from datetime import UTC, datetime
from email.utils import format_datetime
from io import BytesIO
from urllib.error import HTTPError
from urllib.request import Request

import pytest

from connectors.http import HTTPConnector, HTTPSourceError


class _Response:
    def __init__(
        self,
        body: bytes,
        *,
        status: int,
        headers: dict[str, str] | None = None,
    ) -> None:
        self._body = BytesIO(body)
        self.status = status
        self.headers = headers or {}

    def read(self, size: int = -1) -> bytes:
        return self._body.read(size)

    def close(self) -> None:
        self._body.close()

    @property
    def closed(self) -> bool:
        return self._body.closed


class _MockOpener:
    def __init__(self, responses: list[object]) -> None:
        self.responses = iter(responses)
        self.requests: list[Request] = []

    def __call__(self, request: Request, *, timeout: float) -> object:
        assert timeout == 30
        self.requests.append(request)
        response = next(self.responses)
        if isinstance(response, Exception):
            raise response
        return response


def _range_response(body: bytes, start: int, total: int) -> _Response:
    end = start + len(body) - 1
    return _Response(
        body,
        status=206,
        headers={
            "Content-Range": f"bytes {start}-{end}/{total}",
            "Content-Length": str(len(body)),
            "Content-Type": "text/csv",
            "ETag": '"data-etag"',
            "Last-Modified": format_datetime(datetime(2026, 1, 2, tzinfo=UTC), usegmt=True),
        },
    )


def test_http_connector_streams_using_sequential_byte_ranges() -> None:
    responses = [
        _range_response(b"abcd", 0, 10),
        _range_response(b"efgh", 4, 10),
        _range_response(b"ij", 8, 10),
    ]
    opener = _MockOpener(responses)
    connector = HTTPConnector(range_chunk_size=4, opener=opener)

    assert connector.can_handle("https://example.com/data.csv")
    with connector.open("https://example.com/data.csv") as source:
        assert source.metadata.size_bytes == 10
        assert source.metadata.name == "data.csv"
        assert source.metadata.content_type == "text/csv"
        assert source.metadata.etag == '"data-etag"'
        assert source.metadata.modified_at == datetime(2026, 1, 2, tzinfo=UTC)
        assert b"".join(source.read_chunks(3)) == b"abcdefghij"

    assert [request.get_header("Range") for request in opener.requests] == [
        "bytes=0-3",
        "bytes=4-7",
        "bytes=8-9",
    ]
    assert all(response.closed for response in responses)


def test_http_connector_falls_back_to_streaming_when_ranges_are_ignored() -> None:
    response = _Response(
        b"plain response",
        status=200,
        headers={"Content-Length": "14", "Content-Type": "text/plain"},
    )
    opener = _MockOpener([response])
    connector = HTTPConnector(range_chunk_size=4, opener=opener)

    with connector.open("http://example.com/file.txt") as source:
        assert source.metadata.size_bytes == 14
        assert b"".join(source.read_chunks(5)) == b"plain response"

    assert opener.requests[0].get_header("Range") == "bytes=0-3"
    assert response.closed


def test_http_connector_reports_non_200_status_without_leaking_url(caplog) -> None:
    secret = "secret-token"
    opener = _MockOpener(
        [
            HTTPError(
                f"https://example.com/file?token={secret}",
                403,
                "Forbidden",
                {},
                BytesIO(b""),
            )
        ]
    )
    connector = HTTPConnector(opener=opener)

    with pytest.raises(HTTPSourceError, match="HTTP source returned HTTP 403") as error:
        connector.open(f"https://example.com/file?token={secret}")

    assert secret not in str(error.value)
    assert secret not in caplog.text
    assert not caplog.records


@pytest.mark.parametrize(
    "uri",
    [
        "ftp://example.com/file",
        "https:///file",
        "https://user:password@example.com/file",
        "https://example.com:invalid/file",
    ],
)
def test_http_connector_rejects_unsupported_or_credentialed_uris(uri: str) -> None:
    connector = HTTPConnector(opener=_MockOpener([]))

    if uri.startswith("ftp:"):
        assert not connector.can_handle(uri)
    with pytest.raises(ValueError):
        connector.open(uri)


def test_http_connector_redacts_query_from_metadata() -> None:
    response = _Response(b"ok", status=200, headers={"Content-Length": "2"})
    connector = HTTPConnector(opener=_MockOpener([response]))

    with connector.open("https://example.com/data?token=secret") as source:
        assert source.metadata.uri == "https://example.com/data"


def test_http_connector_rejects_invalid_range_response_and_closes_it() -> None:
    response = _Response(b"bad", status=206, headers={"Content-Range": "bytes 1-3/10"})
    connector = HTTPConnector(range_chunk_size=4, opener=_MockOpener([response]))

    with pytest.raises(HTTPSourceError, match="invalid byte range"):
        connector.open("https://example.com/data")

    assert response.closed
