from datetime import UTC, datetime
from io import BytesIO, StringIO

import pytest

from connectors.base import OpenedSource, SourceConnector, SourceMetadata


class InMemoryConnector(SourceConnector):
    def __init__(self, content: bytes) -> None:
        self.content = content
        self.stream: BytesIO | None = None

    def can_handle(self, uri: str) -> bool:
        return uri.startswith("memory://")

    def open(self, uri: str) -> OpenedSource:
        self.stream = BytesIO(self.content)
        metadata = SourceMetadata(
            uri=uri,
            source_type="memory",
            name=uri.rsplit("/", 1)[-1],
            size_bytes=len(self.content),
            modified_at=datetime(2026, 1, 1, tzinfo=UTC),
            content_type="application/octet-stream",
            checksums={"sha256": "test-digest"},
        )
        return OpenedSource(self.stream, metadata)


def test_source_connector_contract_streams_bounded_chunks_and_closes() -> None:
    connector = InMemoryConnector(b"abcdefghij")
    assert connector.can_handle("memory://bucket/file.bin")

    with connector.open("memory://bucket/file.bin") as source:
        assert source.metadata.size_bytes == 10
        assert source.metadata.checksums["sha256"] == "test-digest"
        assert list(source.read_chunks(4)) == [b"abcd", b"efgh", b"ij"]

    assert connector.stream is not None and connector.stream.closed


def test_source_metadata_validates_required_values() -> None:
    with pytest.raises(ValueError, match="uri must not be empty"):
        SourceMetadata(uri="", source_type="local", name="file")
    with pytest.raises(ValueError, match="size_bytes must be zero or greater"):
        SourceMetadata(uri="file:///data", source_type="local", name="data", size_bytes=-1)
    with pytest.raises(ValueError, match="must include a timezone"):
        SourceMetadata(
            uri="file:///data",
            source_type="local",
            name="data",
            modified_at=datetime(2026, 1, 1),
        )


def test_opened_source_rejects_invalid_chunk_size_and_use_after_close() -> None:
    source = OpenedSource(BytesIO(b"data"), SourceMetadata("memory://x", "memory", "x"))

    with pytest.raises(ValueError, match="chunk_size must be greater than zero"):
        list(source.read_chunks(0))

    source.close()
    with pytest.raises(ValueError, match="source stream is closed"):
        list(source.read_chunks(1))


def test_opened_source_rejects_text_streams() -> None:
    source = OpenedSource(StringIO("text"), SourceMetadata("memory://x", "memory", "x"))

    with pytest.raises(TypeError, match="must return bytes"):
        list(source.read_chunks(4))


def test_source_connector_is_an_abstract_contract() -> None:
    with pytest.raises(TypeError):
        SourceConnector()
