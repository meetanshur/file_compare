from datetime import UTC
from pathlib import Path

import pytest

from connectors.local import LocalFileConnector


def test_local_connector_streams_path_and_reports_metadata(tmp_path: Path) -> None:
    source_path = tmp_path / "input file.bin"
    source_path.write_bytes(b"0123456789")
    connector = LocalFileConnector()

    assert connector.can_handle(str(source_path))
    with connector.open(str(source_path)) as source:
        assert source.metadata.name == source_path.name
        assert source.metadata.source_type == "local"
        assert source.metadata.size_bytes == 10
        assert source.metadata.modified_at is not None
        assert source.metadata.modified_at.tzinfo == UTC
        assert source.metadata.uri == source_path.as_uri()
        assert list(source.read_chunks(3)) == [b"012", b"345", b"678", b"9"]


def test_local_connector_opens_file_uri_with_escaped_path(tmp_path: Path) -> None:
    source_path = tmp_path / "space name.txt"
    source_path.write_bytes(b"local")
    connector = LocalFileConnector()

    assert connector.can_handle(source_path.as_uri())
    with connector.open(source_path.as_uri()) as source:
        assert source.metadata.name == "space name.txt"
        assert b"".join(source.read_chunks(10)) == b"local"


def test_local_connector_rejects_non_local_schemes() -> None:
    connector = LocalFileConnector()

    assert not connector.can_handle("s3://bucket/object.csv")
    assert not connector.can_handle("https://example.test/data.csv")
    with pytest.raises(ValueError, match="Not a local file path or file URI"):
        connector.open("s3://bucket/object.csv")


def test_local_connector_reports_missing_path_and_directory(tmp_path: Path) -> None:
    connector = LocalFileConnector()
    with pytest.raises(FileNotFoundError):
        connector.open(str(tmp_path / "missing.txt"))
    with pytest.raises(IsADirectoryError, match="not a regular file"):
        connector.open(str(tmp_path))


def test_local_connector_rejects_file_uri_query_or_fragment(tmp_path: Path) -> None:
    connector = LocalFileConnector()

    with pytest.raises(ValueError, match="query or fragment"):
        connector.open(f"{tmp_path.as_uri()}?download=1")
