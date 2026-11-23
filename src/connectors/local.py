"""Streaming connector for files on the local filesystem."""

from __future__ import annotations

import os
import re
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import unquote, urlsplit

from connectors.base import OpenedSource, SourceConnector, SourceMetadata


class LocalFileConnector(SourceConnector):
    """Open local paths and file URIs as bounded binary streams."""

    def can_handle(self, uri: str) -> bool:
        """Recognize plain paths and file URIs, but not other URI schemes."""
        if os.name == "nt" and re.match(r"^[A-Za-z]:[\\/]", uri):
            return True
        parsed = urlsplit(uri)
        return not parsed.scheme or parsed.scheme.lower() == "file"

    def open(self, uri: str) -> OpenedSource:
        """Open a local file and capture filesystem metadata."""
        if not self.can_handle(uri):
            raise ValueError(f"Not a local file path or file URI: {uri}")

        path = self._path_from_uri(uri)
        stat = path.stat()
        if not path.is_file():
            raise IsADirectoryError(f"Source is not a regular file: {path}")

        stream = path.open("rb")
        try:
            metadata = SourceMetadata(
                uri=path.as_uri(),
                source_type="local",
                name=path.name,
                size_bytes=stat.st_size,
                modified_at=datetime.fromtimestamp(stat.st_mtime, tz=UTC),
                content_type=None,
                etag=None,
                version_id=None,
                checksums={},
            )
        except Exception:
            stream.close()
            raise
        return OpenedSource(stream, metadata)

    @staticmethod
    def _path_from_uri(uri: str) -> Path:
        if os.name == "nt" and re.match(r"^[A-Za-z]:[\\/]", uri):
            return Path(uri).expanduser().resolve()
        parsed = urlsplit(uri)
        if not parsed.scheme:
            return Path(uri).expanduser().resolve()

        if parsed.scheme.lower() != "file":
            raise ValueError(f"Unsupported local URI scheme: {parsed.scheme}")
        if parsed.query or parsed.fragment:
            raise ValueError("file URIs must not include a query or fragment")

        path_text = unquote(parsed.path)
        if parsed.netloc and parsed.netloc.lower() != "localhost":
            path_text = f"//{parsed.netloc}{path_text}"
        elif os.name == "nt" and len(path_text) >= 3 and path_text[0] == "/":
            if path_text[1].isalpha() and path_text[2] == ":":
                path_text = path_text[1:]
        return Path(path_text).expanduser().resolve()
