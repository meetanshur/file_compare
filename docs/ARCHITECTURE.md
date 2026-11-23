# Large-file streaming architecture

This document defines the memory and data-flow rules for comparisons. The
defaults are initial operational limits; later implementation steps must expose
and validate them as application configuration.

## Comparison rules

`ComparisonConfig` is the validated YAML/JSON source for comparison rules.
`strictness: strict` preserves exact parsed values by default. `strictness:
lenient` selects lenient defaults (trim whitespace, case-insensitive text,
null equals empty, ignore XML sibling order, equal supported dates compare
equal, and a 0.01 numeric tolerance); explicitly set fields override those
preset values. The `field_overrides` map accepts exact names or simple glob
patterns and may override ignore, null/empty, case, whitespace, date, and
numeric-tolerance behavior for matching fields. `ignore_fields` likewise
accepts names and glob patterns.

Shared rule resolution and value comparison live in
`common/comparison_rules.py` and are used by record, field, financial, and
reconciliation comparison. The raw comparator remains byte-exact under every
preset: semantic field rules cannot safely erase or normalize bytes without
violating the raw comparison contract. XML schema comparison honors the
`ignore_element_order` rule. Supported date formats are explicitly configured
with `date_formats`; dates remain text comparisons unless
`treat_equal_dates_as_same` is enabled. Financial checks are separately
configured with `financial_rules` and report currency, amount, precision, and
sign findings.

## Resource defaults

| Setting | Default | Meaning |
| --- | ---: | --- |
| `chunk_size` | 4 MiB (4,194,304 bytes) | Maximum byte block read from a source at a time. |
| `max_memory_mb` | 512 MiB per comparison job | Working-memory budget for a job, including buffers, parsers, and DuckDB. |
| `max_differences` | 10,000 findings in an in-memory preview | Maximum findings retained for immediate display in application memory. |
| `max_total_differences` | 1,000,000 findings | Maximum findings persisted per job. `0` means unlimited. |

These are defaults, not user-file size limits. A comparison must not need to
hold either complete input in memory. The memory budget is a target enforced by
bounded buffers, DuckDB memory limits, and job concurrency controls; it is not
a claim that Python or third-party libraries can provide a universal hard
process-memory limit. Tests and benchmarks must measure actual peak memory.

## Data flow

1. Connectors provide seekable or sequential byte streams and metadata. They
   must not download a remote object into a Python `bytes` value.
2. Format, encoding, and line-ending detection uses a bounded source sample;
   content signatures take precedence over filename extensions. Raw comparison
   and hashing read bounded blocks, and raw comparison can stop early when file
   sizes and SHA-256 digests match.
3. Parsers emit records or structural events incrementally. They must not
   build a complete document tree, list of rows, or dictionary of all records.
4. Record data and complete findings are written to job-scoped DuckDB tables.
   Record matching uses database joins rather than unbounded Python maps.
5. UI pages and exports read bounded result batches from DuckDB. A UI preview
   may show at most `max_differences` findings. Persisted findings are available
   through pagination and streaming export up to `max_total_differences`.
6. When the persisted-finding limit is reached, stop comparison early, mark the
   job `truncated`, and clearly report that results are incomplete. Report the
   total finding count when it can be computed without continuing the comparison.

Temporary database files must be kept separate from source files, scoped to a
job, and cleaned up according to job-retention policy. The database must be
configured with a memory limit within the job budget and a temporary directory
with enough free disk space. The job runner assigns DuckDB about 50% of
`max_memory_mb`, leaving the remainder for parsers and buffers. Since records
and findings are persisted in DuckDB, large comparisons can require substantial
free disk space. If disk space is low or a database write fails, fail the job
with a clear error; do not silently fall back to storing all records or
findings in memory.

## Memory rules

- Read no more than `chunk_size` bytes per source read. When processing two
  sources concurrently, account for one active input block per source.
- Keep decoding state across chunk boundaries so split multibyte characters
  are handled correctly. Raw byte comparison must remain independent of text
  decoding.
- Format detection reads no more than its configured sample (64 KiB by
  default). It reports the evidence source, recognized BOM or detected text
  encoding, and line-ending counts observed in that sample; those counts are
  not presented as whole-file totals.
- For large XML files, use `lxml.etree.iterparse` (or SAX); never build a full
  DOM tree. After handling each element, clear it and remove its previous
  siblings so processed XML does not accumulate in memory. XML record parsing
  retains only the selected record being snapshotted, so memory can still grow
  with the size of one individual record. Tabular parsers yield one bounded
  batch or record at a time.
- Parsers implement the shared `Parser` protocol and yield records from binary
  streams. CSV/TSV values remain text; malformed row widths are reported on
  individual records, and any extra field text is retained.
- Record comparison accepts parser records one at a time, converts their fields
  to text, and stores them in bounded DuckDB batches. Field values are retained
  as supplied by the parser; key trimming and case folding affect only the
  matching representation, not the stored source payload. JSON/XML nested
  fields use dotted paths; XML attributes use an `@name` path and element text
  uses `.#text`. XLSX cells use their parser-interpreted value (including the
  formula text for formula cells); exact workbook-byte or lexical-number
  differences remain the responsibility of raw comparison.
- Record keys are exact text by default and may be configured with
  `trim_keys` and `key_case_sensitive`. Duplicate keys produce one warning per
  duplicate key per file. The complete record groups are also compared as
  order-insensitive multisets: differing group sizes and unmatched record
  occurrences are reported with the key and both sides' record data. Duplicate
  keys are not paired arbitrarily or silently skipped.
- Field comparison stores parser values as text alongside their source type.
  JSON number tokens retain their original spelling and JSON scalar categories
  (`string`, `number`, `boolean`, `null`); XLSX values retain the parser's cell
  category (`string`, `number`, `date`, `boolean`, `blank`, or `formula`).
  Each exact, ordered typed row is SHA-256 hashed as it is streamed into
  DuckDB; SQL joins matching keys and reads individual field values only for
  rows whose hashes differ. Comparison rules such as ignored fields,
  null-versus-empty, case, and whitespace are applied after that raw row hash,
  so they cannot conceal changes from the field comparison.
- Reconciliation counts records as they are parsed and accumulates configured
  amount, debit, and credit fields per currency using Python `Decimal`, never
  binary floating-point arithmetic. Configure these with `reconciliation_rules`
  (`currency_field` plus any of `amount_field`, `debit_field`, and
  `credit_field`). Blank, invalid, or non-finite monetary values produce
  row-numbered warnings, are excluded from totals, and increment per-file bad
  amount counters. A currency missing for a valid nonzero amount raises a
  clear error. Currency whitespace and case follow the configured comparison
  rules; totals for different currencies are never combined.
  `numeric_tolerance` defaults to zero (exact comparison) and suppresses
  aggregate differences at or below its configured threshold. Records are
  processed in bounded batches of 1,000, so working memory is bounded by the
  batch and number of distinct currencies, not total input rows.
- JSON array values and YAML documents are materialized one value/document at
  a time; JSON Lines and plain text are processed one line at a time. Fixed-
  width parsing requires configured field widths. Bound the largest record or
  document to keep parser memory within the job budget.
- Do not use whole-file helpers such as `read()`, `read_bytes()`, or
  `read_text()` on comparison inputs. Do not retain all parsed records in
  Python collections.
- Bound parser batches, re-synchronization windows, database fetch batches,
  and concurrent jobs so their combined working sets fit within
  `max_memory_mb`.
- Persist difference detail in DuckDB. `max_differences` limits only the
  in-memory/display preview and never truncates the comparison. The separate
  `max_total_differences` limits persisted findings; `0` disables this limit.
  When reached, stop the job and report its `truncated` status clearly. Report
  the total count where possible, and never describe truncated results as
  complete.
- Stream exports and paginate UI queries. Excel output must apply its supported
  row limit and warn when the complete result cannot fit.
- If an input format or third-party parser cannot meet these rules, report
  that limitation and use a streaming/raw fallback where possible.

## Progress and cancellation

Comparisons run as background jobs. Jobs report status and, when estimable,
bytes or rows processed, percentage, and ETA. A percentage or ETA that cannot
be estimated must be marked unavailable rather than presented as a fabricated
value. Cancellation and failures must close streams and database resources
without losing the job's final status.

## Large-file acceptance checks

The generated 100 MiB, 1 GiB, and 2 GiB XML/CSV fixtures must be compared
without memory growth proportional to input size. Benchmarks record peak
resident memory, elapsed time, bytes or rows processed, and comparison
correctness, including known injected differences. Results are evaluated
against the configured job budget and hardware constraints; benchmark numbers
must state the machine and settings used.

The initial large-file benchmark measures streaming parsing and DuckDB record
ingestion, including hashes, exact sizes, and persisted record counts. It does
not yet measure comparison correctness; rerun the benchmark with the record
comparison joins and expected-difference checks once those are implemented.
After optimizing ingestion, the 100 MB-per-file local baseline measured
48.04 MiB/s for CSV and 25.63 MiB/s for XML, at 334 MiB and 306 MiB peak process
memory respectively, under a 512 MiB budget. These are machine-specific
observations, not performance guarantees; JSON benchmark reports record the
full runtime and settings.

## Raw comparison

`RawComparator` hashes and stages each source in bounded chunks, then yields
edit differences instead of accumulating them. Matching SHA-256 digests and
sizes short-circuit as identical. At a mismatch, the comparator looks ahead
within a configurable bounded window and searches for the smallest insertion,
deletion, or same-length replacement followed by a configurable matching byte
sequence. If none is found, one replaced-region finding covers the window
before comparison resumes. No sequence-diff library is used.

Each finding has byte offsets and lengths, decoded character offsets, one-based
line/column positions, the complete changed byte spans (bounded by the
resynchronization window), and bounded decoded-text previews. Decoding uses an
incremental decoder and can be overridden independently for each input; the
byte finding remains authoritative for exact identity, including BOM and
encoding differences. Character comparison does not normalize Unicode,
whitespace, case, or line endings. `max_total_differences=0` is unlimited;
otherwise `RawComparisonResult` stops when another finding exceeds the limit
and exposes `truncated`, `status`, and a clear message. Pass a job's
`raise_if_cancelled` callback as `cancellation_check`; it is checked while
staging and throughout comparison and lookahead. Result iteration is lazy and
one-shot, and the result should be closed or exhausted to release its
temporary disk files.

Findings are streamed, so callers should persist or consume them incrementally
rather than collecting unbounded results.

## XML schema comparison

`XMLSchemaComparator` consumes the XML parser's record streams in order and
reports element names (including expanded namespace URIs), child hierarchy
and order, attribute-name presence, and missing records/children. It does not
compare element text or attribute values; those are data-level concerns. It
keeps at most one pair of records and an explicit traversal stack in memory.
Pass `max_total_differences` to bound emitted findings and
`cancellation_check` to cooperatively cancel a job.

When an XSD is supplied, `validate_xml_schema` validates the source with
lxml's incremental `iterparse` schema validation. External entity resolution,
network access, and DTD loading are disabled. The XSD itself is parsed with
the same restrictions. Malformed or invalid XML raises `XMLSchemaError`
including the source and schema names.

## Metadata comparison

`MetadataComparator` compares file size, timezone-normalized modification
time, optional source-system and version identifiers, and MD5/SHA-256
checksums. A missing value on either side has status `unavailable`, not
`different`. Timestamps are compared as UTC instants. Checksums supplied by a
connector are reused; hashes not already available are computed in bounded
chunks. Pass the raw comparison result to reuse its SHA-256 digest and other
computed checksums without rereading the file. Source-system and version
values are optional `SourceMetadata` fields; connector-specific `version_id`
is used when an explicit version is absent.

On Windows with Python 3.13, a three-run comparison of the generated
`xml_100MB_A.xml` and `xml_100MB_B.xml` fixtures (100,000,000 bytes each)
verified both fixture hashes and the expected changed-record values in the
raw finding previews. Median elapsed time was 1.433 seconds, or 133.11 MiB/s
across both inputs, with a 72.44 MiB peak process working set. The raw report
contained two 28-byte replaced regions; those regions cover the manifest's
insert, update, and delete records, which are record-level changes rather than
three necessarily distinct byte edits. This is a machine- and cache-specific
measurement, not a performance guarantee.
