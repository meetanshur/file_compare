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
   may show at most `max_differences` findings; all findings remain available
   through pagination and streaming export.

Temporary database files must be kept separate from source files, scoped to a
job, and cleaned up according to job-retention policy. The database must be
configured with a memory limit within the job budget and a temporary directory
with enough free disk space. Insufficient disk space or an I/O failure must
fail the job explicitly; the engine must not silently fall back to storing all
records in memory.

## Memory rules

- Read no more than `chunk_size` bytes per source read. When processing two
  sources concurrently, account for one active input block per source.
- Keep decoding state across chunk boundaries so split multibyte characters
  are handled correctly. Raw byte comparison must remain independent of text
  decoding.
- Use iterative XML parsing and release completed elements. Tabular parsers
  yield one bounded batch or record at a time.
- Do not use whole-file helpers such as `read()`, `read_bytes()`, or
  `read_text()` on comparison inputs. Do not retain all parsed records in
  Python collections.
- Bound parser batches, re-synchronization windows, database fetch batches,
  and concurrent jobs so their combined working sets fit within
  `max_memory_mb`.
- Persist full difference detail in DuckDB. `max_differences` limits only an
  in-memory/display preview; it must never truncate comparison, stored results,
  total counts, or exports.
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
