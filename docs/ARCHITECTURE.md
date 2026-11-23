# Large-file streaming architecture

This document defines the memory and data-flow rules for comparisons. The
defaults are initial operational limits; later implementation steps must expose
and validate them as application configuration.

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
2. Detection and hashing read bounded blocks. Raw comparison can stop early
   when file sizes and SHA-256 digests match.
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
- For large XML files, use `lxml.etree.iterparse` (or SAX); never build a full
  DOM tree. After handling each element, clear it and remove its previous
  siblings so processed XML does not accumulate in memory. Tabular parsers
  yield one bounded batch or record at a time.
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
