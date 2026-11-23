"""Pluggable streaming file-source connectors."""

from connectors.azure_blob import AzureBlobConnector, AzureBlobSourceError
from connectors.base import OpenedSource, SourceConnector, SourceMetadata
from connectors.http import HTTPConnector, HTTPSourceError
from connectors.local import LocalFileConnector
from connectors.s3 import S3Connector, S3SourceError
from connectors.sftp import SFTPConnector, SFTPSourceError

__all__ = [
    "AzureBlobConnector",
    "AzureBlobSourceError",
    "HTTPConnector",
    "HTTPSourceError",
    "LocalFileConnector",
    "OpenedSource",
    "S3Connector",
    "S3SourceError",
    "SFTPConnector",
    "SFTPSourceError",
    "SourceConnector",
    "SourceMetadata",
]
