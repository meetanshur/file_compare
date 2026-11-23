"""Generate fixed-size, streaming XML and CSV comparison fixtures."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path

DEFAULT_CHUNK_SIZE = 1024 * 1024
DEFAULT_ROW_WIDTH = 1024
SIZE_UNITS = {
    "B": 1,
    "KB": 1_000,
    "MB": 1_000_000,
    "GB": 1_000_000_000,
    "KIB": 1024,
    "MIB": 1024**2,
    "GIB": 1024**3,
}
SIZE_PATTERN = re.compile(r"^(?P<amount>[1-9]\d*)(?P<unit>B|KB|MB|GB|KiB|MiB|GiB)?$", re.IGNORECASE)


@dataclass(frozen=True)
class GeneratedPair:
    """Paths and manifest for a generated comparison fixture pair."""

    file_a: Path
    file_b: Path
    manifest: Path
    size_bytes: int
    record_count: int


def parse_size(value: str) -> tuple[int, str]:
    """Parse a positive byte count with an optional SI or IEC suffix."""
    match = SIZE_PATTERN.fullmatch(value)
    if match is None:
        raise ValueError(
            "Size must be a positive integer with optional B, KB, MB, GB, KiB, MiB, or GiB"
        )
    amount = int(match.group("amount"))
    unit = match.group("unit") or "B"
    multiplier = SIZE_UNITS[unit.upper()]
    return amount * multiplier, value.upper()


def generate_pair(
    output_directory: str | Path,
    file_format: str,
    size_bytes: int,
    *,
    size_label: str | None = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    row_width: int = DEFAULT_ROW_WIDTH,
) -> GeneratedPair:
    """Stream a same-size file pair and its expected-differences JSON manifest.

    The output directory must be named `.test_data` so generated fixtures stay
    under the repository's ignored test-data folder.
    """
    output_path = Path(output_directory)
    if output_path.name != ".test_data":
        raise ValueError("Generated fixtures must be written into a .test_data directory")
    if file_format not in {"xml", "csv"}:
        raise ValueError("file_format must be 'xml' or 'csv'")
    if size_bytes < 1:
        raise ValueError("size_bytes must be greater than zero")
    if row_width < 1:
        raise ValueError("row_width must be greater than zero")
    if chunk_size < 1:
        raise ValueError("chunk_size must be greater than zero")

    output_path.mkdir(parents=True, exist_ok=True)
    display_size = size_label or f"{size_bytes}B"
    safe_size_label = re.sub(r"[^A-Za-z0-9]+", "", display_size)
    if not safe_size_label:
        raise ValueError("size_label must contain at least one alphanumeric character")

    if file_format == "csv":
        return _generate_csv_pair(output_path, size_bytes, safe_size_label, chunk_size, row_width)
    return _generate_xml_pair(output_path, size_bytes, safe_size_label, chunk_size, row_width)


def _record_value(record_id: int, *, updated: bool = False) -> str:
    prefix = "change" if updated else "value-"
    return f"{prefix}{record_id:012d}"


def _generate_csv_pair(
    output_directory: Path,
    size_bytes: int,
    size_label: str,
    chunk_size: int,
    row_width: int,
) -> GeneratedPair:
    header = b"id,value,padding\n"
    base_row = _csv_row(0, _record_value(0), row_width)
    width = max(row_width, len(base_row))
    record_count, final_padding = _calculate_record_count(size_bytes, len(header), width)
    file_a = output_directory / f"csv_{size_label}_A.csv"
    file_b = output_directory / f"csv_{size_label}_B.csv"
    manifest = output_directory / f"csv_{size_label}_expected.json"

    def rows(*, target: bool) -> Iterator[bytes]:
        delete_id = record_count - 2
        update_id = record_count - 1
        for record_id in range(record_count):
            if target and record_id == delete_id:
                continue
            value = _record_value(record_id, updated=target and record_id == update_id)
            padding = final_padding if record_id == 0 else 0
            yield _csv_row(record_id, value, width, extra_padding=padding)
        if target:
            yield _csv_row(record_count, _record_value(record_count), width)

    digest_a = _write_stream(file_a, rows(target=False), header, b"", chunk_size)
    digest_b = _write_stream(file_b, rows(target=True), header, b"", chunk_size)
    _write_manifest(
        manifest,
        file_format="csv",
        size_bytes=size_bytes,
        file_a=file_a,
        file_b=file_b,
        digest_a=digest_a,
        digest_b=digest_b,
        record_count=record_count,
    )
    return GeneratedPair(file_a, file_b, manifest, size_bytes, record_count)


def _csv_row(record_id: int, value: str, width: int, *, extra_padding: int = 0) -> bytes:
    prefix = f"{record_id:012d},{value},".encode("ascii")
    padding_length = width - len(prefix) - 1 + extra_padding
    if padding_length < 0:
        raise ValueError("row_width is too small for generated CSV fields")
    return prefix + (b" " * padding_length) + b"\n"


def _generate_xml_pair(
    output_directory: Path,
    size_bytes: int,
    size_label: str,
    chunk_size: int,
    row_width: int,
) -> GeneratedPair:
    opening = b"<records>\n"
    closing = b"</records>\n"
    base_row = _xml_row(0, _record_value(0), row_width)
    width = max(row_width, len(base_row))
    record_count, final_padding = _calculate_record_count(
        size_bytes, len(opening) + len(closing), width
    )
    file_a = output_directory / f"xml_{size_label}_A.xml"
    file_b = output_directory / f"xml_{size_label}_B.xml"
    manifest = output_directory / f"xml_{size_label}_expected.json"

    def rows(*, target: bool) -> Iterator[bytes]:
        delete_id = record_count - 2
        update_id = record_count - 1
        for record_id in range(record_count):
            if target and record_id == delete_id:
                continue
            value = _record_value(record_id, updated=target and record_id == update_id)
            padding = final_padding if record_id == 0 else 0
            yield _xml_row(record_id, value, width, extra_padding=padding)
        if target:
            yield _xml_row(record_count, _record_value(record_count), width)

    digest_a = _write_stream(file_a, rows(target=False), opening, closing, chunk_size)
    digest_b = _write_stream(file_b, rows(target=True), opening, closing, chunk_size)
    _write_manifest(
        manifest,
        file_format="xml",
        size_bytes=size_bytes,
        file_a=file_a,
        file_b=file_b,
        digest_a=digest_a,
        digest_b=digest_b,
        record_count=record_count,
    )
    return GeneratedPair(file_a, file_b, manifest, size_bytes, record_count)


def _xml_row(
    record_id: int,
    value: str,
    width: int,
    *,
    extra_padding: int = 0,
) -> bytes:
    prefix = f'<record id="{record_id:012d}"><value>{value}</value><padding>'.encode("ascii")
    suffix = b"</padding></record>\n"
    padding_length = width - len(prefix) - len(suffix) + extra_padding
    if padding_length < 0:
        raise ValueError("row_width is too small for generated XML fields")
    return prefix + (b" " * padding_length) + suffix


def _calculate_record_count(
    size_bytes: int, fixed_overhead: int, row_width: int
) -> tuple[int, int]:
    record_count = (size_bytes - fixed_overhead) // row_width
    if record_count < 3:
        minimum = fixed_overhead + 3 * row_width
        raise ValueError(f"size_bytes must be at least {minimum} for three known differences")
    final_padding = size_bytes - fixed_overhead - record_count * row_width
    return record_count, final_padding


def _write_stream(
    path: Path,
    rows: Iterable[bytes],
    prefix: bytes,
    suffix: bytes,
    chunk_size: int,
) -> str:
    """Write row bytes incrementally while retaining only one output chunk."""
    if chunk_size < 1:
        raise ValueError("chunk_size must be greater than zero")
    digest = hashlib.sha256()
    with path.open("wb") as stream:
        buffer = bytearray()

        def append(data: bytes) -> None:
            buffer.extend(data)
            if len(buffer) >= chunk_size:
                stream.write(buffer)
                digest.update(buffer)
                buffer.clear()

        append(prefix)
        for row in rows:
            append(row)
        append(suffix)
        if buffer:
            stream.write(buffer)
            digest.update(buffer)
    return digest.hexdigest()


def _write_manifest(
    path: Path,
    *,
    file_format: str,
    size_bytes: int,
    file_a: Path,
    file_b: Path,
    digest_a: str,
    digest_b: str,
    record_count: int,
) -> None:
    delete_key = f"{record_count - 2:012d}"
    update_key = f"{record_count - 1:012d}"
    insert_key = f"{record_count:012d}"
    manifest = {
        "schema_version": 1,
        "format": file_format,
        "size_bytes_per_file": size_bytes,
        "record_counts": {"file_a": record_count, "file_b": record_count},
        "files": {
            "file_a": {"name": file_a.name, "sha256": digest_a},
            "file_b": {"name": file_b.name, "sha256": digest_b},
        },
        "expected_differences": {
            "insert": [{"key": insert_key}],
            "update": [
                {
                    "key": update_key,
                    "field": "value",
                    "value_a": _record_value(record_count - 1),
                    "value_b": _record_value(record_count - 1, updated=True),
                }
            ],
            "delete": [{"key": delete_key}],
        },
    }
    with path.open("w", encoding="utf-8", newline="\n") as stream:
        json.dump(manifest, stream, indent=2)
        stream.write("\n")


def _parse_cli_size(value: str) -> tuple[int, str]:
    try:
        return parse_size(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def _default_output_directory() -> Path:
    return Path(__file__).resolve().parents[2] / ".test_data"


def main(argv: list[str] | None = None) -> int:
    """Run the test-data generator command-line interface."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--size",
        required=True,
        type=_parse_cli_size,
        metavar="SIZE",
        help="exact size of each file, e.g. 100MB, 1GiB, or 2GB",
    )
    parser.add_argument(
        "--format",
        choices=("xml", "csv", "both"),
        default="both",
        help="file format to generate (default: both)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=_default_output_directory(),
        help="ignored .test_data directory (default: repository .test_data)",
    )
    args = parser.parse_args(argv)
    size_bytes, size_label = args.size

    if args.output_dir.name != ".test_data":
        parser.error("--output-dir must be named .test_data so generated files remain ignored")

    formats = ("xml", "csv") if args.format == "both" else (args.format,)
    try:
        for file_format in formats:
            pair = generate_pair(
                args.output_dir,
                file_format,
                size_bytes,
                size_label=size_label,
            )
            print(
                f"Generated {file_format.upper()} pair: {pair.file_a}, {pair.file_b}; "
                f"manifest: {pair.manifest}; size: {pair.size_bytes} bytes each; "
                f"records: {pair.record_count} each."
            )
    except (OSError, ValueError) as exc:
        print(f"Could not generate test files: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
