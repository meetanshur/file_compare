# Source connectors

All source adapters implement `SourceConnector` and return an `OpenedSource`
from `open(uri)`. The opened source provides `SourceMetadata` and a context
manager around its binary stream. Consume it using `read_chunks(chunk_size)` or
pass its stream to an iterative parser. Do not read the entire source into
memory.

The local connector accepts Windows/POSIX paths and `file://` URIs:

```python
from connectors.local import LocalFileConnector

connector = LocalFileConnector()
with connector.open(r"C:\data\input.xml") as source:
    print(source.metadata.size_bytes)
    for chunk in source.read_chunks(4 * 1024 * 1024):
        process(chunk)
```

The S3 connector accepts `s3://bucket/key` URIs and streams the object body:

```python
from connectors.s3 import S3Connector

connector = S3Connector()
with connector.open("s3://my-bucket/path/input.xml") as source:
    print(source.metadata.size_bytes, source.metadata.etag)
    for chunk in source.read_chunks(4 * 1024 * 1024):
        process(chunk)
```

S3 authentication uses boto3's standard credential provider chain (for example
environment credentials, shared AWS config/credentials files, or an attached
role). Do not put credentials in the URI or application config. An optional
`region_name` can be provided to `S3Connector`; otherwise boto3 resolves the
region using its normal configuration. The connector uses `HeadObject` for
metadata and `GetObject` for the streaming body, and closes the body when the
opened source exits its context.

`SourceMetadata` contains a normalized URI, source type, name, optional byte
size, timezone-aware modification time, content type, ETag/version identifiers,
and optional checksum values. Values a connector cannot provide remain unset;
connectors must not fabricate metadata.

## Adding a connector

Implement `can_handle(uri)` and `open(uri)`. The latter should open a binary
stream, gather available metadata without reading the full file, and return it
wrapped in `OpenedSource`. Network clients should stream their response body
and close the response when the opened source closes. Raise a clear exception
for invalid paths, permissions, authentication, and transport failures.
