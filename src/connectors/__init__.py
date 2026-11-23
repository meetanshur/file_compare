"""Pluggable streaming file-source connectors."""

from connectors.base import OpenedSource, SourceConnector, SourceMetadata
from connectors.local import LocalFileConnector
from connectors.s3 import S3Connector, S3SourceError

__all__ = [
    "LocalFileConnector",
    "OpenedSource",
    "S3Connector",
    "S3SourceError",
    "SourceConnector",
    "SourceMetadata",
]
