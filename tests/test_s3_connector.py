from datetime import UTC, datetime
from io import BytesIO
from unittest.mock import Mock

import pytest
from botocore.exceptions import ClientError

from connectors.s3 import S3Connector, S3SourceError


def _head_response() -> dict[str, object]:
    return {
        "ContentLength": 11,
        "LastModified": datetime(2026, 1, 2, 3, 4, tzinfo=UTC),
        "ContentType": "application/xml",
        "ETag": '"etag-value"',
        "VersionId": "version-1",
        "ChecksumSHA256": "checksum-value",
    }


def test_s3_connector_streams_object_and_exposes_head_metadata() -> None:
    body = BytesIO(b"<root>data</root>")
    client = Mock()
    client.head_object.return_value = _head_response()
    client.get_object.return_value = {"Body": body}
    connector = S3Connector(client=client)

    assert connector.can_handle("s3://example-bucket/folder/data%20file.xml")
    with connector.open("s3://example-bucket/folder/data%20file.xml") as source:
        assert source.metadata.name == "data file.xml"
        assert source.metadata.source_type == "s3"
        assert source.metadata.size_bytes == 11
        assert source.metadata.modified_at == datetime(2026, 1, 2, 3, 4, tzinfo=UTC)
        assert source.metadata.content_type == "application/xml"
        assert source.metadata.etag == '"etag-value"'
        assert source.metadata.version_id == "version-1"
        assert source.metadata.checksums == {"sha256": "checksum-value"}
        assert list(source.read_chunks(5)) == [b"<root", b">data", b"</roo", b"t>"]

    assert body.closed
    client.head_object.assert_called_once_with(Bucket="example-bucket", Key="folder/data file.xml")
    client.get_object.assert_called_once_with(Bucket="example-bucket", Key="folder/data file.xml")


def test_s3_connector_uses_boto3_default_credentials(monkeypatch) -> None:
    body = BytesIO(b"data")
    client = Mock()
    client.head_object.return_value = _head_response()
    client.get_object.return_value = {"Body": body}
    boto_client = Mock(return_value=client)
    monkeypatch.setattr("connectors.s3.boto3.client", boto_client)

    with S3Connector(region_name="us-east-1").open("s3://bucket/key") as source:
        assert b"".join(source.read_chunks(8)) == b"data"

    boto_client.assert_called_once_with("s3", region_name="us-east-1")


@pytest.mark.parametrize(
    ("uri", "message"),
    [
        ("https://bucket/key", "Not an S3 URI"),
        ("s3:///key", "include a bucket"),
        ("s3://bucket", "include an object key"),
        ("s3://bucket/key?versionId=x", "query or fragment"),
        ("s3://user@bucket/key", "user information or a port"),
    ],
)
def test_s3_connector_rejects_invalid_uris(uri: str, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        S3Connector._parse_uri(uri)


def test_s3_connector_wraps_aws_errors_without_exposing_credentials() -> None:
    client = Mock()
    client.head_object.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "denied"}},
        "HeadObject",
    )
    connector = S3Connector(client=client)

    with pytest.raises(S3SourceError, match="s3://bucket/key: AWS error AccessDenied"):
        connector.open("s3://bucket/key")

    client.get_object.assert_not_called()


def test_s3_connector_closes_body_if_metadata_is_invalid() -> None:
    body = BytesIO(b"data")
    client = Mock()
    client.head_object.return_value = {"ContentLength": -1}
    client.get_object.return_value = {"Body": body}

    with pytest.raises(ValueError, match="S3 ContentLength must be a nonnegative integer"):
        S3Connector(client=client).open("s3://bucket/key")

    assert body.closed
