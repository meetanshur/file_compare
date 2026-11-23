from __future__ import annotations

import stat
from datetime import UTC, datetime
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import Mock, patch

import paramiko
import pytest

from connectors.sftp import SFTPConnector, SFTPSourceError


def _mock_client(data: bytes = b"streamed sftp file"):
    remote_file = BytesIO(data)
    sftp = Mock()
    sftp.stat.return_value = SimpleNamespace(
        st_mode=stat.S_IFREG | 0o644,
        st_size=len(data),
        st_mtime=1_767_312_000,
    )
    sftp.open.return_value = remote_file
    client = Mock()
    client.open_sftp.return_value = sftp
    return client, sftp, remote_file


def test_sftp_connector_streams_file_and_closes_remote_resources() -> None:
    client, sftp, remote_file = _mock_client()
    with patch("connectors.sftp.paramiko.SSHClient", return_value=client) as factory:
        connector = SFTPConnector(timeout=12)

        assert connector.can_handle("sftp://user@example.com:2222/incoming/data.csv")
        with connector.open("sftp://user@example.com:2222/incoming/data.csv") as source:
            assert source.metadata.name == "data.csv"
            assert source.metadata.source_type == "sftp"
            assert source.metadata.size_bytes == len(b"streamed sftp file")
            assert source.metadata.modified_at == datetime.fromtimestamp(1_767_312_000, tz=UTC)
            assert source.metadata.uri == "sftp://example.com:2222/incoming/data.csv"
            assert b"".join(source.read_chunks(4)) == b"streamed sftp file"

        factory.assert_called_once_with()
        client.load_system_host_keys.assert_called_once_with()
        client.set_missing_host_key_policy.assert_called_once()
        assert isinstance(
            client.set_missing_host_key_policy.call_args.args[0],
            paramiko.RejectPolicy,
        )
        client.connect.assert_called_once_with(
            hostname="example.com",
            port=2222,
            username="user",
            timeout=12,
            banner_timeout=12,
            auth_timeout=12,
            allow_agent=True,
            look_for_keys=True,
        )
        sftp.stat.assert_called_once_with("/incoming/data.csv")
        sftp.open.assert_called_once_with("/incoming/data.csv", "rb")

    assert remote_file.closed
    sftp.close.assert_called_once_with()
    client.close.assert_called_once_with()


@pytest.mark.parametrize(
    ("uri", "message"),
    [
        ("https://example.com/file", "absolute sftp"),
        ("sftp:///file", "absolute sftp"),
        ("sftp://example.com", "include a file path"),
        ("sftp://user:password@example.com/file", "must not contain a password"),
        ("sftp://example.com/file?token=secret", "query or fragment"),
        ("sftp://example.com:bad/file", "URI is invalid"),
    ],
)
def test_sftp_connector_rejects_invalid_uris(uri: str, message: str) -> None:
    with patch("connectors.sftp.paramiko.SSHClient") as factory:
        with pytest.raises(ValueError, match=message):
            SFTPConnector._parse_uri(uri)
    factory.assert_not_called()


def test_sftp_connector_reports_connection_error_without_uri_credentials() -> None:
    client = Mock()
    client.connect.side_effect = paramiko.AuthenticationException("secret password")
    with patch("connectors.sftp.paramiko.SSHClient", return_value=client):
        with pytest.raises(SFTPSourceError, match="AuthenticationException") as error:
            SFTPConnector().open("sftp://user@example.com/private/data.csv")

    assert "secret password" not in str(error.value)
    assert "user@" not in str(error.value)
    client.close.assert_called_once_with()


def test_sftp_connector_closes_session_when_remote_path_is_missing() -> None:
    client, sftp, _ = _mock_client()
    sftp.stat.side_effect = FileNotFoundError
    with patch("connectors.sftp.paramiko.SSHClient", return_value=client):
        with pytest.raises(SFTPSourceError, match="FileNotFoundError"):
            SFTPConnector().open("sftp://example.com/missing.csv")

    sftp.close.assert_called_once_with()
    client.close.assert_called_once_with()


def test_sftp_connector_rejects_remote_directories_and_closes_session() -> None:
    client, sftp, _ = _mock_client()
    sftp.stat.return_value.st_mode = stat.S_IFDIR | 0o755
    with patch("connectors.sftp.paramiko.SSHClient", return_value=client):
        with pytest.raises(SFTPSourceError, match="not a regular file"):
            SFTPConnector().open("sftp://example.com/incoming")

    sftp.close.assert_called_once_with()
    client.close.assert_called_once_with()
