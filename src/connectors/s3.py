"""Streaming connector for objects in AWS S3."""

from __future__ import annotations

from datetime import datetime
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import unquote, urlsplit

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from connectors.base import OpenedSource, SourceConnector, SourceMetadata


class S3SourceError(RuntimeError):
    """An S3 source could not be opened."""


class S3Connector(SourceConnector):
    """Open S3 objects through boto3's standard credential provider chain."""

    def __init__(self, *, client: Any | None = None, region_name: str | None = None) -> None:
        self._client = client
        self._region_name = region_name

    def can_handle(self, uri: str) -> bool:
        """Return whether the URI uses the S3 scheme."""
        return urlsplit(uri).scheme.lower() == "s3"

    def open(self, uri: str) -> OpenedSource:
        """Open an S3 object stream and attach metadata from HEAD response."""
        bucket, key = self._parse_uri(uri)
        client = self._client or boto3.client("s3", region_name=self._region_name)

        try:
            response = client.head_object(Bucket=bucket, Key=key)
            body = client.get_object(Bucket=bucket, Key=key)["Body"]
        except ClientError as exc:
            error = exc.response.get("Error", {})
            code = str(error.get("Code", "Unknown"))
            raise S3SourceError(f"Could not open S3 source {uri}: AWS error {code}") from exc
        except BotoCoreError as exc:
            raise S3SourceError(f"Could not open S3 source {uri}: AWS client error") from exc

        try:
            metadata = SourceMetadata(
                uri=uri,
                source_type="s3",
                name=PurePosixPath(key).name or key,
                size_bytes=_optional_nonnegative_int(response.get("ContentLength")),
                modified_at=_aware_datetime(response.get("LastModified")),
                content_type=_optional_string(response.get("ContentType")),
                etag=_optional_string(response.get("ETag")),
                version_id=_optional_string(response.get("VersionId")),
                checksums={
                    algorithm.lower(): value
                    for algorithm, value in (
                        ("sha1", response.get("ChecksumSHA1")),
                        ("sha256", response.get("ChecksumSHA256")),
                    )
                    if isinstance(value, str)
                },
            )
        except (TypeError, ValueError):
            body.close()
            raise

        return OpenedSource(body, metadata)

    @staticmethod
    def _parse_uri(uri: str) -> tuple[str, str]:
        parsed = urlsplit(uri)
        if parsed.scheme.lower() != "s3":
            raise ValueError(f"Not an S3 URI: {uri}")
        if not parsed.netloc:
            raise ValueError(f"S3 URI must include a bucket: {uri}")
        if parsed.username is not None or parsed.password is not None or parsed.port is not None:
            raise ValueError(f"S3 URI must not include user information or a port: {uri}")
        if parsed.query or parsed.fragment:
            raise ValueError(f"S3 URI must not include a query or fragment: {uri}")

        bucket = parsed.netloc
        key = unquote(parsed.path.lstrip("/"))
        if not key:
            raise ValueError(f"S3 URI must include an object key: {uri}")
        return bucket, key


def _optional_string(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _optional_nonnegative_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return value
    raise ValueError("S3 ContentLength must be a nonnegative integer")


def _aware_datetime(value: object) -> datetime | None:
    if not isinstance(value, datetime):
        return None
    if value.utcoffset() is None:
        return None
    return value
