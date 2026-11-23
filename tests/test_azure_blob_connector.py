from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
from azure.core.exceptions import ResourceNotFoundError

from connectors.azure_blob import AzureBlobConnector, AzureBlobSourceError


def _mock_service(data: bytes = b"streamed azure blob"):
    downloader = Mock()
    downloader.chunks.return_value = iter((data[:5], data[5:]))
    properties = SimpleNamespace(
        size=len(data),
        last_modified=datetime(2026, 1, 2, 3, 4, tzinfo=UTC),
        content_settings=SimpleNamespace(content_type="text/csv"),
        etag='"blob-etag"',
        version_id="version-2",
    )
    blob = Mock()
    blob.get_blob_properties.return_value = properties
    blob.download_blob.return_value = downloader
    service = Mock()
    service.get_blob_client.return_value = blob
    return service, blob, downloader


def test_azure_blob_connector_streams_and_exposes_blob_metadata() -> None:
    service, blob, downloader = _mock_service()
    connector = AzureBlobConnector(service_client=service)

    assert connector.can_handle("azure://myaccount/incoming/folder/data.csv")
    assert connector.can_handle("https://myaccount.blob.core.windows.net/incoming/folder/data.csv")
    with connector.open("azure://myaccount/incoming/folder/data.csv") as source:
        assert source.metadata.name == "data.csv"
        assert source.metadata.source_type == "azure_blob"
        assert source.metadata.size_bytes == len(b"streamed azure blob")
        assert source.metadata.modified_at == datetime(2026, 1, 2, 3, 4, tzinfo=UTC)
        assert source.metadata.content_type == "text/csv"
        assert source.metadata.etag == '"blob-etag"'
        assert source.metadata.version_id == "version-2"
        assert b"".join(source.read_chunks(3)) == b"streamed azure blob"

    service.get_blob_client.assert_called_once_with(
        container="incoming",
        blob="folder/data.csv",
    )
    blob.download_blob.assert_called_once_with(max_concurrency=1)
    downloader.close.assert_called_once_with()
    service.close.assert_not_called()


def test_azure_blob_connector_parses_https_uri_and_owns_sdk_resources() -> None:
    service, blob, _ = _mock_service(b"data")
    credential = Mock()
    blob_service_factory = Mock(return_value=service)
    with (
        patch("connectors.azure_blob.BlobServiceClient", blob_service_factory),
        patch("connectors.azure_blob.DefaultAzureCredential", return_value=credential),
    ):
        connector = AzureBlobConnector()
        with connector.open("https://myaccount.blob.core.windows.net/container/file.csv") as source:
            assert source.metadata.uri == (
                "https://myaccount.blob.core.windows.net/container/file.csv"
            )
            assert b"".join(source.read_chunks(8)) == b"data"

    blob_service_factory.assert_called_once_with(
        "https://myaccount.blob.core.windows.net",
        credential=credential,
    )
    service.close.assert_called_once_with()
    credential.close.assert_called_once_with()


@pytest.mark.parametrize(
    ("uri", "message"),
    [
        ("https://example.com/container/blob", "must use azure://"),
        ("azure:///container/blob", "include an account"),
        ("azure://account/container", "include a container and blob"),
        ("azure://account/container/blob?sig=secret", "query or fragment"),
        ("azure://user:password@account/container/blob", "user information"),
        ("https://account.blob.core.windows.net/container/blob#fragment", "query or fragment"),
        ("https://account.blob.core.windows.net:443/container/blob", "user information"),
    ],
)
def test_azure_blob_connector_rejects_invalid_uris(uri: str, message: str) -> None:
    service = Mock()
    with pytest.raises(ValueError, match=message):
        AzureBlobConnector(service_client=service).open(uri)
    service.get_blob_client.assert_not_called()


def test_azure_blob_connector_wraps_sdk_errors_without_leaking_uri_secrets() -> None:
    service = Mock()
    service.get_blob_client.side_effect = ResourceNotFoundError(message="private error details")
    with pytest.raises(AzureBlobSourceError, match="ResourceNotFoundError") as error:
        AzureBlobConnector(service_client=service).open("azure://account/container/blob")

    assert "private error details" not in str(error.value)
    service.close.assert_not_called()


def test_azure_blob_connector_closes_resources_on_open_failure() -> None:
    service, blob, _ = _mock_service()
    blob.get_blob_properties.side_effect = ResourceNotFoundError()
    credential = Mock()
    with (
        patch("connectors.azure_blob.BlobServiceClient", return_value=service),
        patch("connectors.azure_blob.DefaultAzureCredential", return_value=credential),
    ):
        with pytest.raises(AzureBlobSourceError):
            AzureBlobConnector().open("azure://account/container/blob")

    service.close.assert_called_once_with()
    credential.close.assert_called_once_with()


def test_azure_blob_stream_read_all_is_supported_without_reopening_blob() -> None:
    service, _, downloader = _mock_service(b"abc")
    downloader.chunks.return_value = iter((b"a", b"bc"))
    with AzureBlobConnector(service_client=service).open(
        "azure://account/container/blob"
    ) as source:
        assert source.stream.read() == b"abc"


def test_azure_blob_connector_reports_streaming_errors() -> None:
    service, _, downloader = _mock_service()

    def failing_chunks():
        yield b"first"
        raise ResourceNotFoundError()

    downloader.chunks.return_value = failing_chunks()
    with AzureBlobConnector(service_client=service).open(
        "azure://account/container/blob"
    ) as source:
        with pytest.raises(AzureBlobSourceError, match="Azure Blob download failed"):
            list(source.read_chunks(10))

    downloader.close.assert_called_once_with()
