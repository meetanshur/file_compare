from __future__ import annotations

from io import BytesIO

import pytest

from comparators import RawComparator, RawDifference, RawDifferenceKind


def test_identical_streams_have_no_raw_differences() -> None:
    differences = list(
        RawComparator(chunk_size=4, sample_size_bytes=3).compare(
            BytesIO(b"same\r\ntext"),
            BytesIO(b"same\r\ntext"),
        )
    )

    assert differences == []


def test_raw_comparison_reports_byte_and_character_positions() -> None:
    differences = list(
        RawComparator(filename_a="a.txt", filename_b="b.txt", chunk_size=4).compare(
            BytesIO(b"first,\r\nsecond"),
            BytesIO(b"first.\r\nsecond"),
        )
    )

    assert differences == [
        _difference(
            RawDifferenceKind.BYTE,
            offset_a=5,
            offset_b=5,
            line_a=1,
            column_a=6,
            line_b=1,
            column_b=6,
            value_a=ord(","),
            value_b=ord("."),
        ),
        _difference(
            RawDifferenceKind.CHARACTER,
            offset_a=5,
            offset_b=5,
            line_a=1,
            column_a=6,
            line_b=1,
            column_b=6,
            value_a=",",
            value_b=".",
        ),
    ]


def test_raw_comparison_keeps_byte_and_decoded_character_results_distinct() -> None:
    differences = list(
        RawComparator(encoding_a="utf-8-sig", encoding_b="utf-8").compare(
            BytesIO(b"\xef\xbb\xbffile"),
            BytesIO(b"file"),
        )
    )

    assert len(differences) == 7
    assert all(difference.kind is RawDifferenceKind.BYTE for difference in differences)
    assert [difference.offset_a for difference in differences] == list(range(7))
    assert [difference.offset_b for difference in differences] == [0, 1, 2, 3, None, None, None]
    assert [difference.value_b for difference in differences] == [
        ord("f"),
        ord("i"),
        ord("l"),
        ord("e"),
        None,
        None,
        None,
    ]


def test_raw_comparison_handles_multibyte_characters_across_chunks() -> None:
    differences = list(
        RawComparator(
            encoding_a="utf-8",
            encoding_b="utf-8",
            chunk_size=2,
            sample_size_bytes=1,
        ).compare(
            BytesIO("é".encode()),
            BytesIO("ê".encode()),
        )
    )

    assert [difference.kind for difference in differences] == [
        RawDifferenceKind.BYTE,
        RawDifferenceKind.CHARACTER,
    ]
    character_difference = differences[-1]
    assert character_difference.offset_a == character_difference.offset_b == 0
    assert character_difference.value_a == "é"
    assert character_difference.value_b == "ê"


def test_raw_comparison_reports_extra_bytes_and_characters_at_eof() -> None:
    differences = list(
        RawComparator(encoding_a="utf-8", encoding_b="utf-8", chunk_size=2).compare(
            BytesIO(b"a\n"),
            BytesIO(b"a\nb"),
        )
    )

    assert differences[-2].kind is RawDifferenceKind.BYTE
    assert differences[-2].offset_a is None
    assert differences[-2].offset_b == 2
    assert differences[-2].line_b == 2
    assert differences[-2].column_b == 1
    assert differences[-1].kind is RawDifferenceKind.CHARACTER
    assert differences[-1].offset_a is None
    assert differences[-1].offset_b == 2
    assert differences[-1].line_b == 2
    assert differences[-1].column_b == 1


def test_raw_comparison_detects_line_ending_differences() -> None:
    differences = list(
        RawComparator(encoding_a="utf-8", encoding_b="utf-8").compare(
            BytesIO(b"a\r\nb"),
            BytesIO(b"a\nb"),
        )
    )

    byte_differences = [item for item in differences if item.kind is RawDifferenceKind.BYTE]
    character_differences = [
        item for item in differences if item.kind is RawDifferenceKind.CHARACTER
    ]
    assert [item.value_a for item in byte_differences] == [13, 10, ord("b")]
    assert [item.value_b for item in byte_differences] == [10, ord("b"), None]
    assert [item.value_a for item in character_differences] == ["\r", "\n", "b"]
    assert [item.value_b for item in character_differences] == ["\n", "b", None]
    assert character_differences[0].line_a == character_differences[0].line_b == 1


def test_raw_comparison_reads_only_bounded_chunks() -> None:
    class ShortReadStream:
        def __init__(self, value: bytes) -> None:
            self.stream = BytesIO(value)
            self.requested_sizes: list[int] = []

        def read(self, size: int = -1) -> bytes:
            self.requested_sizes.append(size)
            return self.stream.read(min(size, 2))

    source_a = ShortReadStream(b"abcdefgh")
    source_b = ShortReadStream(b"abcdEfgh")

    list(RawComparator(chunk_size=3, sample_size_bytes=4).compare(source_a, source_b))

    assert source_a.requested_sizes and source_b.requested_sizes
    assert max(source_a.requested_sizes) <= 3
    assert max(source_b.requested_sizes) <= 3


@pytest.mark.parametrize(
    ("chunk_size", "sample_size_bytes"),
    [(0, 1), (1, 0)],
)
def test_raw_comparator_rejects_invalid_buffer_sizes(
    chunk_size: int,
    sample_size_bytes: int,
) -> None:
    with pytest.raises(ValueError):
        RawComparator(chunk_size=chunk_size, sample_size_bytes=sample_size_bytes)


def _difference(
    kind: RawDifferenceKind,
    *,
    offset_a: int | None,
    offset_b: int | None,
    line_a: int | None,
    column_a: int | None,
    line_b: int | None,
    column_b: int | None,
    value_a: int | str | None,
    value_b: int | str | None,
) -> RawDifference:
    return RawDifference(
        kind=kind,
        offset_a=offset_a,
        offset_b=offset_b,
        line_a=line_a,
        column_a=column_a,
        line_b=line_b,
        column_b=column_b,
        value_a=value_a,
        value_b=value_b,
    )
