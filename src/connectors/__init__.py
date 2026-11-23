"""Pluggable streaming file-source connectors."""

from connectors.base import OpenedSource, SourceConnector, SourceMetadata
from connectors.local import LocalFileConnector

__all__ = ["LocalFileConnector", "OpenedSource", "SourceConnector", "SourceMetadata"]
