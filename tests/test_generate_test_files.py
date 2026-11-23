import csv
import hashlib
import json
import xml.etree.ElementTree as ET

import pytest

from tools.generate_test_files import generate_pair, main, parse_size


@pytest.mark.parametrize("file_format", ["xml", "csv"])
def test_generator_creates_exact_size_pair_and_manifest(tmp_path, file_format) -> None:
    output_directory = tmp_path / ".test_data"
    requested_size = 8_192

    pair = generate_pair(
        output_directory,
        file_format,
        requested_size,
        chunk_size=113,
        row_width=128,
    )

    assert pair.file_a.stat().st_size == requested_size
    assert pair.file_b.stat().st_size == requested_size
    manifest = json.loads(pair.manifest.read_text(encoding="utf-8"))
    assert manifest["format"] == file_format
    assert manifest["size_bytes_per_file"] == requested_size
    assert manifest["files"]["file_a"]["name"] == pair.file_a.name
    assert manifest["files"]["file_b"]["name"] == pair.file_b.name
    with pair.file_a.open("rb") as stream:
        assert (
            hashlib.file_digest(stream, "sha256").hexdigest()
            == manifest["files"]["file_a"]["sha256"]
        )
    with pair.file_b.open("rb") as stream:
        assert (
            hashlib.file_digest(stream, "sha256").hexdigest()
            == manifest["files"]["file_b"]["sha256"]
        )
    assert set(manifest["expected_differences"]) == {"insert", "update", "delete"}
    assert len(manifest["expected_differences"]["insert"]) == 1
    assert len(manifest["expected_differences"]["update"]) == 1
    assert len(manifest["expected_differences"]["delete"]) == 1

    if file_format == "xml":
        root_a = ET.parse(pair.file_a).getroot()
        root_b = ET.parse(pair.file_b).getroot()
        assert len(root_a) == pair.record_count
        assert len(root_b) == pair.record_count
        rows_a = {row.attrib["id"]: row for row in root_a}
        rows_b = {row.attrib["id"]: row for row in root_b}
        assert set(rows_b) - set(rows_a) == {manifest["expected_differences"]["insert"][0]["key"]}
        assert set(rows_a) - set(rows_b) == {manifest["expected_differences"]["delete"][0]["key"]}
        for key in set(rows_a) & set(rows_b):
            assert rows_a[key].findtext("padding") == rows_b[key].findtext("padding")
        update = manifest["expected_differences"]["update"][0]
        assert rows_a[update["key"]].findtext("value") == update["value_a"]
        assert rows_b[update["key"]].findtext("value") == update["value_b"]
    else:
        with pair.file_a.open(newline="", encoding="ascii") as stream:
            rows_a = list(csv.DictReader(stream))
        with pair.file_b.open(newline="", encoding="ascii") as stream:
            rows_b = list(csv.DictReader(stream))
        assert len(rows_a) == pair.record_count
        assert len(rows_b) == pair.record_count
        indexed_a = {row["id"]: row for row in rows_a}
        indexed_b = {row["id"]: row for row in rows_b}
        assert set(indexed_b) - set(indexed_a) == {
            manifest["expected_differences"]["insert"][0]["key"]
        }
        assert set(indexed_a) - set(indexed_b) == {
            manifest["expected_differences"]["delete"][0]["key"]
        }
        for key in set(indexed_a) & set(indexed_b):
            assert indexed_a[key]["padding"] == indexed_b[key]["padding"]
        update = manifest["expected_differences"]["update"][0]
        assert indexed_a[update["key"]]["value"] == update["value_a"]
        assert indexed_b[update["key"]]["value"] == update["value_b"]


def test_parse_size_supports_decimal_and_binary_units() -> None:
    assert parse_size("100MB") == (100_000_000, "100MB")
    assert parse_size("1GiB") == (1024**3, "1GIB")


def test_cli_accepts_size_and_writes_only_to_test_data_directory(tmp_path, capsys) -> None:
    output_directory = tmp_path / ".test_data"

    assert (
        main(
            [
                "--size",
                "8192B",
                "--format",
                "csv",
                "--output-dir",
                str(output_directory),
            ]
        )
        == 0
    )

    output = capsys.readouterr().out
    assert "size: 8192 bytes each" in output
    assert (output_directory / "csv_8192B_A.csv").stat().st_size == 8192
    assert (output_directory / "csv_8192B_B.csv").stat().st_size == 8192
    assert (output_directory / "csv_8192B_expected.json").is_file()


def test_generator_requires_ignored_test_data_directory(tmp_path) -> None:
    with pytest.raises(ValueError, match="written into a .test_data directory"):
        generate_pair(tmp_path / "fixtures", "csv", 8_192)
