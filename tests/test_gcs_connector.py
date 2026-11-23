from __future__ import annotations

from datetime import UTC, datetime
from io import BytesIO
from unittest.mock import Mock, patch

import pytest
from google.api_core.exceptions import NotFound

from connectors.gcs import GCSConnector, GCSSourceError


def _mock_client(data: bytes = b"streamed gcs object"):
    reader = BytesIO(data)
    blob = Mock()
    blob.size = len(data)
    blob.updated = datetime(2026, 1, 2, 3, 4, tzinfo=UTC)
    blob.content_type = "text/csv"
    blob.etag = '"object-etag"'
    blob.generation = 123456
    blob.open.return_value = reader
    bucket = Mock()
    bucket.blob.return_value = blob
    client = Mock()
    client.bucket.return_value = bucket
    return client, bucket, blob, reader


def test_gcs_connector_streams_object_and_exposes_metadata() -> None:
    client, bucket, blob, reader = _mock_client()
    connector = GCSConnector(client=client, chunk_size=256 * 1024)

    assert connector.can_handle("gs://my-bucket/incoming/folder/data.csv")
    with connector.open("gs://my-bucket/incoming/folder/data.csv") as source:
        assert source.metadata.name == "data.csv"
        assert source.metadata.source_type == "gcs"
        assert source.metadata.size_bytes == len(b"streamed gcs object")
        assert source.metadata.modified_at == datetime(2026, 1, 2, 3, 4, tzinfo=UTC)
        assert source.metadata.content_type == "text/csv"
        assert source.metadata.etag == '"object-etag"'
        assert source.metadata.version_id == "123456"
        assert b"".join(source.read_chunks(4)) == b"streamed gcs object"

    client.bucket.assert_called_once_with("my-bucket")
    bucket.blob.assert_called_once_with("incoming/folder/data.csv")
    blob.reload.assert_called_once_with()
    blob.open.assert_called_once_with("rb", chunk_size=256 * 1024)
    assert reader.closed
    client.close.assert_not_called()


def test_gcs_connector_uses_application_default_credentials_and_closes_owned_client() -> None:
    client, _, _, reader = _mock_client(b"data")
    with patch("connectors.gcs.storage.Client", return_value=client) as factory:
        with GCSConnector().open("gs://bucket/path/file") as source:
            assert source.metadata.uri == "gs://bucket/path/file"
            assert b"".join(source.read_chunks(8)) == b"data"

    factory.assert_called_once_with()
    assert reader.closed
    client.close.assert_called_once_with()


@pytest.mark.parametrize(
    ("uri", "message"),
    [
        ("https://storage.googleapis.com/bucket/object", "must use gs://"),
        ("gs:///object", "must use gs://"),
        ("gs://bucket", "include an object name"),
        ("gs://user:password@bucket/object", "user information"),
        ("gs://bucket/object?token=secret", "query or fragment"),
        ("gs://bucket:bad/object", "URI is invalid"),
    ],
)
def test_gcs_connector_rejects_invalid_uris(uri: str, message: str) -> None:
    client = Mock()
    with pytest.raises(ValueError, match=message):
        GCSConnector(client=client).open(uri)
    client.bucket.assert_not_called()


def test_gcs_connector_wraps_api_errors_without_disclosing_details() -> None:
    client, _, blob, _ = _mock_client()
    blob.reload.side_effect = NotFound("private response details")

    with pytest.raises(GCSSourceError, match="NotFound") as error:
        GCSConnector(client=client).open("gs://bucket/private-object")

    assert "private response details" not in str(error.value)
    client.close.assert_not_called()


def test_gcs_connector_closes_reader_and_owned_client_on_open_failure() -> None:
    client, _, blob, _ = _mock_client()
    blob.open.side_effect = NotFound("object not found")
    with patch("connectors.gcs.storage.Client", return_value=client):
        with pytest.raises(GCSSourceError):
            GCSConnector().open("gs://bucket/object")

    client.close.assert_called_once_with()


def test_gcs_connector_wraps_errors_during_streaming() -> None:
    client, _, blob, _ = _mock_client()
    reader = Mock()
    reader.read.side_effect = NotFound("object no longer available")
    blob.open.return_value = reader
    with GCSConnector(client=client).open("gs://bucket/object") as source:
        with pytest.raises(GCSSourceError, match="Google Cloud Storage download failed"):
            source.stream.read(1)

    reader.close.assert_called_once_with()
