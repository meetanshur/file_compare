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
