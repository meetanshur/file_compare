"""Streaming connector for files served over SFTP."""

from __future__ import annotations

import io
import stat
from datetime import UTC, datetime
from pathlib import PurePosixPath
from typing import BinaryIO, cast
from urllib.parse import unquote, urlsplit

import paramiko

from connectors.base import OpenedSource, SourceConnector, SourceMetadata


class SFTPSourceError(RuntimeError):
    """An SFTP source could not be opened or read."""


class SFTPConnector(SourceConnector):
    """Open SFTP files using SSH agent/default keys and verified host keys."""

    def __init__(self, *, timeout: float = 30) -> None:
        if timeout <= 0:
            raise ValueError("timeout must be greater than zero")
        self._timeout = timeout

    def can_handle(self, uri: str) -> bool:
        """Return whether the URI uses the SFTP scheme."""
        try:
            return urlsplit(uri).scheme.lower() == "sftp"
        except ValueError:
            return False

    def open(self, uri: str) -> OpenedSource:
        """Connect to a host-verified SFTP server and open a binary file."""
        hostname, port, username, path = self._parse_uri(uri)
        client = paramiko.SSHClient()
        client.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.RejectPolicy())

        sftp = None
        remote_file = None
        try:
            client.connect(
                hostname=hostname,
                port=port,
                username=username,
                timeout=self._timeout,
                banner_timeout=self._timeout,
                auth_timeout=self._timeout,
                allow_agent=True,
                look_for_keys=True,
            )
            sftp = client.open_sftp()
            file_stat = sftp.stat(path)
            if not stat.S_ISREG(file_stat.st_mode):
                raise SFTPSourceError(
                    f"SFTP source is not a regular file on {_authority(hostname, port)}"
                )
            remote_file = sftp.open(path, "rb")
            metadata = SourceMetadata(
                uri=_safe_uri(hostname, port, path),
                source_type="sftp",
                name=PurePosixPath(path).name,
                size_bytes=_optional_nonnegative_int(file_stat.st_size),
                modified_at=_modified_at(file_stat.st_mtime),
            )
        except SFTPSourceError:
            _close_resources(remote_file, sftp, client)
            raise
        except (OSError, EOFError, paramiko.SSHException, ValueError) as exc:
            _close_resources(remote_file, sftp, client)
            raise SFTPSourceError(
                f"Could not open SFTP source on {_authority(hostname, port)} ({type(exc).__name__})"
            ) from None

        stream = _SFTPStream(remote_file, sftp, client)
        return OpenedSource(cast(BinaryIO, stream), metadata)

    @staticmethod
    def _parse_uri(uri: str) -> tuple[str, int, str | None, str]:
        return _parse_sftp_uri(uri)


class _SFTPStream(io.RawIOBase):
    """Own and close the remote file and its SFTP/SSH sessions."""

    def __init__(self, remote_file, sftp, client) -> None:
        super().__init__()
        self._remote_file = remote_file
        self._sftp = sftp
        self._client = client

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        if self.closed:
            raise ValueError("I/O operation on closed SFTP source")
        return self._remote_file.read(size)

    def close(self) -> None:
        if self.closed:
            return
        try:
            _close_resources(self._remote_file, self._sftp, self._client)
        finally:
            super().close()


def _parse_sftp_uri(uri: str) -> tuple[str, int, str | None, str]:
    try:
        parsed = urlsplit(uri)
        port = parsed.port
    except ValueError:
        raise ValueError("SFTP source URI is invalid") from None

    if parsed.scheme.lower() != "sftp" or not parsed.hostname:
        raise ValueError("SFTP source URI must be an absolute sftp:// URL")
    if parsed.password is not None:
        raise ValueError("SFTP source URI must not contain a password")
    if parsed.query or parsed.fragment:
        raise ValueError("SFTP source URI must not include a query or fragment")
    if port is not None and not 1 <= port <= 65535:
        raise ValueError("SFTP source URI port is invalid")

    path = unquote(parsed.path)
    if not path or path == "/":
        raise ValueError("SFTP source URI must include a file path")
    return parsed.hostname, port or 22, unquote(parsed.username) if parsed.username else None, path


def _safe_uri(hostname: str, port: int, path: str) -> str:
    return f"sftp://{_authority(hostname, port)}{path}"


def _authority(hostname: str, port: int) -> str:
    if ":" in hostname:
        hostname = f"[{hostname}]"
    return f"{hostname}:{port}"


def _optional_nonnegative_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return value
    raise ValueError("SFTP file size must be a nonnegative integer")


def _modified_at(value: object) -> datetime | None:
    if not isinstance(value, (int, float)):
        return None
    return datetime.fromtimestamp(value, tz=UTC)


def _close_resources(remote_file, sftp, client) -> None:
    try:
        if remote_file is not None:
            remote_file.close()
    finally:
        try:
            if sftp is not None:
                sftp.close()
        finally:
            client.close()
