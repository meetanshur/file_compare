"""Pluggable streaming file-source connectors."""

from connectors.base import OpenedSource, SourceConnector, SourceMetadata
from connectors.http import HTTPConnector, HTTPSourceError
from connectors.local import LocalFileConnector
from connectors.s3 import S3Connector, S3SourceError

__all__ = [
    "HTTPConnector",
    "HTTPSourceError",
    "LocalFileConnector",
    "OpenedSource",
    "S3Connector",
    "S3SourceError",
    "SourceConnector",
    "SourceMetadata",
]
