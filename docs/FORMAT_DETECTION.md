# Format and text detection

`parsers.detection.detect_file(sample, filename)` inspects a bounded byte
sample and returns a `FileDetection` with the likely format, detection source,
encoding, binary/text classification, and line-ending style/counts. Pass the
source name when available so the extension can be used as fallback evidence.
The default inspection bound is 64 KiB and can be changed with
`sample_size_bytes`.

Content signatures and recognizable text structure take precedence over the
filename extension. The detector recognizes XML, JSON, CSV, TSV, XLSX, PDF,
YAML document markers, and common text extensions. Unknown binary input is
reported as `binary`; unrecognized text is reported as `text`. An ambiguous or
empty sample may be `unknown`.

Encoding detection checks common UTF BOMs first, then validates UTF-8, then
uses `charset-normalizer` for other text encodings. Binary data has no encoding
reported. Line-ending counts distinguish CRLF, LF, and CR, including mixed
samples. Counts describe only the supplied sample and must not be interpreted
as whole-file totals. Keep the sample bounded (64 KiB by default in the
comparison architecture), and keep raw comparison on the original bytes so
encoding, BOM, and line-ending differences remain observable.

```python
from parsers.detection import detect_file

sample = b'{"id": 1}\r\n'
detection = detect_file(sample, "records.txt")
print(detection.file_format)  # json: content structure overrides the extension
print(detection.encoding, detection.line_ending, detection.line_ending_counts)
```

## Streaming XML records

Use `parsers.xml.iter_xml_records()` for XML inputs. By default it emits each
direct child of the document root as one `XmlRecord`; pass `record_tag` to
select elements by exact qualified name or local name. Each result contains an
immutable element snapshot with ordered attributes and children, element text,
and tail text. XML text and whitespace are retained as parsed; raw byte
comparison remains responsible for byte-level encoding, BOM, and line-ending
differences.

The parser uses `lxml.etree.iterparse` with network access and entity
resolution disabled. It clears each processed record and removes its previous
siblings. Memory is bounded by the largest selected record, so a single
enormous XML record can still require substantial memory. The caller owns and
must close the source stream. Malformed input raises `XMLParseError` with the
source name and line number.

```python
from parsers.xml import iter_xml_records

with open("records.xml", "rb") as stream:
    for record in iter_xml_records(stream, record_tag="record", source_name="records.xml"):
        process(record.element)
```

## Streaming CSV and TSV records

`CSVParser`, `TSVParser`, and `DelimitedTextParser` implement the common
`Parser` protocol: `parse(binary_source)` incrementally yields records and
never loads the complete input. The delimited parsers use Step 14 format and
encoding detection by default. `format_override` and `encoding_override`
provide explicit user overrides; the delimiter and header behavior are also
configurable.

Every parsed data field is retained as text without trimming or numeric
coercion. Standard CSV quoting is interpreted: quote delimiters are removed,
escaped quote characters are decoded, and the resulting field text—including
spaces—is preserved. Empty or duplicate header names are assigned generated
unique names to keep row mappings unambiguous. With `has_header=False`, columns
are named `column_1`, `column_2`, and so on.

Rows with too few columns receive a `too_few_columns` warning and contain only
the values present. Rows with extra columns receive a `too_many_columns`
warning; extra field text remains available in `extra_values`. Malformed CSV
syntax and decode failures raise `DelimitedParseError`.

```python
from parsers.delimited import CSVParser

parser = CSVParser(filename="input.csv", encoding_override="utf-8", has_header=True)
with open("input.csv", "rb") as stream:
    for record in parser.parse(stream):
        process(record.values, record.warnings)
```

## JSON, YAML, fixed-width, and plain text

The other Step 17 parsers also implement `Parser.parse(binary_source)` and use
the shared bounded encoding detector. Set `encoding_override` when detection
is incorrect or when the source's encoding is known. `JSONParser` emits one
record per value for a top-level JSON array; a top-level object or scalar is a
single record. Set `json_lines=True` to parse newline-delimited JSON. Array
parsing holds only one value at a time, so one exceptionally large JSON value
can still require substantial memory.

JSON integer and floating-point number tokens are returned as their original
text, preserving lexical distinctions such as `1.10`, `1E2`, and long integer
digits. The parser reads and decodes each top-level array value before yielding
it, rather than building the complete array. Its memory use is therefore
bounded by the largest individual array value plus parser buffers; a single
very large value can still use substantial memory.

`YAMLParser` uses a safe loader with implicit scalar resolvers disabled. Plain
scalars such as `007`, `1.10`, `yes`, `no`, `null`, and `2024-01-05` remain
their original text rather than being converted to numbers, booleans, null, or
dates. Each YAML document is one record; a single large document is still
materialized as one value. Unsafe Python object constructors are not enabled.

`FixedWidthParser` requires widths as a sequence or a name-to-width mapping.
Widths count decoded characters, not bytes. Spaces inside fields are retained.
Short rows carry a `too_few_characters` warning; extra row text is retained in
`extra_text` with a `too_many_characters` warning.

`PlainTextParser` yields one physical line per record, including the original
line terminator and all leading/trailing whitespace. Raw byte comparison
remains necessary for byte-level encoding and BOM differences.

## Streaming XLSX rows

`XLSXParser` implements the shared `Parser` protocol and uses openpyxl's
read-only worksheet iteration. It accepts a seekable or non-seekable binary
source, spools the compressed workbook to a temporary file using bounded
1 MiB reads, then yields one row at a time. Set `sheet_name` to select one
worksheet; otherwise worksheets are read in workbook order. The first row is
the header by default; `has_header=False` generates `column_1`, `column_2`,
and so on. `format_override="xlsx"` can be used when the extension/content
sample is ambiguous.

Each mapped value is an `XLSXCellValue` with the openpyxl-interpreted value,
cell data type (`string`, `number`, `date`, `boolean`, `blank`, or `formula`),
and Excel `number_format`. Formula cells expose the formula in both `value`
and `formula`, plus `cached_value` from a second read-only pass with
`data_only=True`. Cached results are only present if the workbook itself
contains them; formulas are not recalculated. Numeric values are returned as
interpreted by openpyxl, so their original XML lexical spelling (for example
whether the source wrote `1.10` or `1.1`) is not retained. Use raw comparison
for exact workbook bytes and formatting differences. Every row includes its
worksheet name.

Rows are read incrementally, but the compressed file is temporarily stored on
disk and openpyxl may retain the workbook shared-string table in memory.
The workbook is opened twice to pair formula cells with cached values, so
shared-string memory can be roughly doubled. Memory is therefore not
guaranteed to be independent of workbook size. Ensure the temporary directory
has enough free space and avoid oversized shared-string tables within a
constrained memory budget. The input stream remains owned by the caller.

```python
from parsers.structured import FixedWidthParser, JSONParser, PlainTextParser, YAMLParser

with open("records.json", "rb") as stream:
    for record in JSONParser(filename="records.json").parse(stream):
        process(record.value)

with open("legacy.dat", "rb") as stream:
    for record in FixedWidthParser(
        {"account": 8, "amount": 12},
        filename="legacy.dat",
        encoding_override="utf-8",
    ).parse(stream):
        process(record.values, record.warnings)

from parsers.xlsx import XLSXParser

with open("workbook.xlsx", "rb") as stream:
    for record in XLSXParser(filename="workbook.xlsx", sheet_name="Records").parse(stream):
        process(record.values)
```
