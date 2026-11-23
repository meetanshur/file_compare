from __future__ import annotations

import random
import string
from io import BytesIO

import pytest

from comparators import (
    RawComparator,
    RawComparisonStatus,
    RawDifferenceKind,
)


def test_identical_size_and_sha256_return_immediate_identical_result() -> None:
    result = RawComparator(chunk_size=4).compare(
        BytesIO(b"identical\r\ntext"),
        BytesIO(b"identical\r\ntext"),
    )

    assert result.is_identical
    assert result.identical
    assert result.status is RawComparisonStatus.IDENTICAL
    assert result.message == "Files are identical."
    assert list(result) == []


@pytest.mark.parametrize(
    ("edit", "expected_kind", "length_a", "length_b"),
    [
        ("insert", RawDifferenceKind.INSERT, 0, 1),
        ("delete", RawDifferenceKind.DELETE, 1, 0),
        ("replace", RawDifferenceKind.REPLACED, 1, 1),
    ],
)
def test_random_input_resynchronizes_after_one_edit(
    edit: str,
    expected_kind: RawDifferenceKind,
    length_a: int,
    length_b: int,
) -> None:
    original = _random_ascii(10_000)
    position = 4_321
    if edit == "insert":
        changed = original[:position] + b"," + original[position:]
    elif edit == "delete":
        changed = original[:position] + original[position + 1 :]
    else:
        replacement = b"!" if original[position : position + 1] != b"!" else b"?"
        changed = original[:position] + replacement + original[position + 1 :]

    result = _compare(original, changed)
    differences = list(result)

    assert len(differences) == 1
    assert differences[0].kind is expected_kind
    assert differences[0].offset_a == position
    assert differences[0].offset_b == position
    assert differences[0].length_a == length_a
    assert differences[0].length_b == length_b
    assert result.status is RawComparisonStatus.COMPLETE


def test_random_input_reports_two_separate_edits_as_two_differences() -> None:
    original = _random_ascii(10_000)
    changed = bytearray(original)
    changed[1_234] = ord("!")
    changed[7_654] = ord("?")

    differences = list(_compare(original, bytes(changed)))

    assert len(differences) == 2
    assert [difference.offset_a for difference in differences] == [1_234, 7_654]
    assert all(difference.kind is RawDifferenceKind.REPLACED for difference in differences)


@pytest.mark.parametrize(
    ("data_a", "data_b", "encoding_a", "encoding_b", "kind", "length_a", "length_b"),
    [
        (b"invoice,amount", b"invoice.amount", "utf-8", "utf-8", "replaced", 1, 1),
        (b"extra space", b"extra  space", "utf-8", "utf-8", "insert", 0, 1),
        (b"Case change", b"case change", "utf-8", "utf-8", "replaced", 1, 1),
        (b"file", b"\xef\xbb\xbffile", "utf-8", "utf-8", "insert", 0, 3),
        (b"line\nend", b"line\r\nend", "utf-8", "utf-8", "insert", 0, 1),
        (
            "café".encode("latin-1"),
            "café".encode(),
            "latin-1",
            "utf-8",
            "replaced",
            1,
            2,
        ),
    ],
)
def test_raw_comparison_reports_common_text_edits_once(
    data_a: bytes,
    data_b: bytes,
    encoding_a: str,
    encoding_b: str,
    kind: str,
    length_a: int,
    length_b: int,
) -> None:
    differences = list(
        RawComparator(
            encoding_a=encoding_a,
            encoding_b=encoding_b,
            resync_window_bytes=128,
        ).compare(BytesIO(data_a), BytesIO(data_b))
    )

    assert len(differences) == 1
    assert differences[0].kind.value == kind
    assert differences[0].length_a == length_a
    assert differences[0].length_b == length_b
    if encoding_a == "latin-1":
        assert differences[0].text_a == differences[0].text_b == "é"


def test_character_positions_are_reported_for_a_multiline_edit() -> None:
    difference = next(
        iter(
            RawComparator(encoding_a="utf-8", encoding_b="utf-8").compare(
                BytesIO(b"first\nsecond"),
                BytesIO(b"first\nSecond"),
            )
        )
    )

    assert difference.offset_a == difference.offset_b == 6
    assert difference.character_offset_a == difference.character_offset_b == 6
    assert difference.line_a == difference.line_b == 2
    assert difference.column_a == difference.column_b == 1
    assert difference.text_a == "s"
    assert difference.text_b == "S"


def test_character_position_after_bare_carriage_return_starts_next_line() -> None:
    difference = next(
        iter(
            RawComparator(encoding_a="utf-8", encoding_b="utf-8").compare(
                BytesIO(b"a\rX"),
                BytesIO(b"a\rY"),
            )
        )
    )

    assert difference.line_a == difference.line_b == 2
    assert difference.column_a == difference.column_b == 1


def test_max_total_differences_stops_and_marks_result_truncated() -> None:
    original = _random_ascii(1_000)
    changed = bytearray(original)
    changed[100] = ord("!")
    changed[800] = ord("?")
    result = RawComparator(max_total_differences=1).compare(
        BytesIO(original),
        BytesIO(changed),
    )

    assert len(list(result)) == 1
    assert result.truncated
    assert result.status is RawComparisonStatus.TRUNCATED
    assert "truncated" in result.message.lower()


def test_cancellation_callback_runs_during_comparison_and_lookahead() -> None:
    original = _random_ascii(10_000)
    changed = bytearray(original)
    changed[100] = ord("!")

    class TrackedStream:
        def __init__(self, value: bytes) -> None:
            self.stream = BytesIO(value)
            self.position = 0

        def read(self, size: int = -1) -> bytes:
            chunk = self.stream.read(size)
            self.position += len(chunk)
            return chunk

    source_a = TrackedStream(original)
    source_b = TrackedStream(bytes(changed))
    both_sources_consumed_checks = 0

    def cancel_after_staging() -> None:
        nonlocal both_sources_consumed_checks
        if source_a.position == len(original) and source_b.position == len(changed):
            both_sources_consumed_checks += 1
            if both_sources_consumed_checks == 2:
                raise RuntimeError("cancelled")

    result = RawComparator(chunk_size=128).compare(
        source_a,
        source_b,
        cancellation_check=cancel_after_staging,
    )

    with pytest.raises(RuntimeError, match="cancelled"):
        list(result)
    assert both_sources_consumed_checks == 2


def test_no_resynchronization_reports_one_bounded_replaced_region() -> None:
    data_a = _random_ascii(512)
    data_b = _random_ascii(512)[::-1]
    result = RawComparator(
        encoding_a="ascii",
        encoding_b="ascii",
        resync_window_bytes=8,
        sync_bytes=4,
        max_total_differences=1,
    ).compare(BytesIO(data_a), BytesIO(data_b))

    with result:
        first = next(iter(result))

    assert first.kind is RawDifferenceKind.REPLACED
    assert first.length_a == first.length_b == 8
    assert len(first.value_a) <= 256
    assert len(first.value_b) <= 256


def test_raw_comparison_handles_nonseekable_short_read_streams() -> None:
    class ShortReadStream:
        def __init__(self, value: bytes) -> None:
            self.stream = BytesIO(value)
            self.requested_sizes: list[int] = []

        def read(self, size: int = -1) -> bytes:
            self.requested_sizes.append(size)
            return self.stream.read(min(size, 2))

    source_a = ShortReadStream(_random_ascii(100))
    source_b = ShortReadStream(_random_ascii(100))
    result = RawComparator(chunk_size=7).compare(source_a, source_b)
    list(result)

    assert max(source_a.requested_sizes) <= 7
    assert max(source_b.requested_sizes) <= 7


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"chunk_size": 0}, "chunk_size"),
        ({"sample_size_bytes": 0}, "sample_size_bytes"),
        ({"resync_window_bytes": 0}, "resync_window_bytes"),
        ({"sync_bytes": 0}, "sync_bytes"),
        ({"max_total_differences": -1}, "max_total_differences"),
    ],
)
def test_raw_comparator_rejects_invalid_limits(kwargs: dict, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        RawComparator(**kwargs)


def _compare(data_a: bytes, data_b: bytes):
    return RawComparator(
        encoding_a="ascii",
        encoding_b="ascii",
        chunk_size=257,
        resync_window_bytes=64,
        sync_bytes=32,
    ).compare(BytesIO(data_a), BytesIO(data_b))


def _random_ascii(length: int) -> bytes:
    generator = random.Random(20261124)
    return "".join(generator.choices(string.ascii_letters + string.digits, k=length)).encode(
        "ascii"
    )
