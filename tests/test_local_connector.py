from datetime import UTC
from pathlib import Path
from unittest.mock import patch

import pytest

from connectors.local import LocalFileConnector


def test_local_connector_streams_path_and_reports_metadata(tmp_path: Path) -> None:
    source_path = tmp_path / "my folder" / "input file.bin"
    source_path.parent.mkdir()
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


def test_local_connector_reports_missing_path(tmp_path: Path) -> None:
    connector = LocalFileConnector()
    missing_path = tmp_path / "my folder" / "missing.txt"
    with pytest.raises(FileNotFoundError) as error:
        connector.open(str(missing_path))
    assert str(missing_path) in str(error.value)


def test_local_connector_reports_directory_path(tmp_path: Path) -> None:
    connector = LocalFileConnector()
    directory_path = tmp_path / "my folder"
    directory_path.mkdir()
    with pytest.raises(IsADirectoryError) as error:
        connector.open(str(directory_path))
    assert str(directory_path) in str(error.value)


def test_local_connector_reports_permission_denied_path(tmp_path: Path) -> None:
    connector = LocalFileConnector()
    source_path = tmp_path / "my folder" / "private.txt"
    source_path.parent.mkdir()
    source_path.write_text("data", encoding="utf-8")
    permission_error = PermissionError("access denied")

    with patch.object(Path, "open", side_effect=permission_error):
        with pytest.raises(PermissionError) as error:
            connector.open(str(source_path))

    assert str(source_path) in str(error.value)
    assert error.value.__cause__ is permission_error


def test_local_connector_rejects_file_uri_query_or_fragment(tmp_path: Path) -> None:
    connector = LocalFileConnector()

    with pytest.raises(ValueError, match="query or fragment"):
        connector.open(f"{tmp_path.as_uri()}?download=1")
