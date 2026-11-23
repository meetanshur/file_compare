# Large test-file generator

Generated fixtures belong in the repository's ignored `.test_data/` folder.
The generator only accepts output directories named `.test_data` and defaults
to the repository root's `.test_data/`. Do not commit generated fixture files.

From the repository root, generate an exact 100 MB pair for each supported
format:

```powershell
python -m tools.generate_test_files --size 100MB --format both
```

The generator accepts a positive integer size with byte, decimal, or binary
units. For example, `100MB` is 100,000,000 bytes, while `100MiB` is
104,857,600 bytes. Larger examples:

```powershell
python -m tools.generate_test_files --size 1GiB --format xml
python -m tools.generate_test_files --size 2GB --format csv
```

Each invocation creates File A, File B, and an adjacent
`*_expected.json` manifest for each selected format. The manifest includes
SHA-256 digests, record counts, and the expected one insert, one update, and
one delete. Files are written incrementally in bounded chunks and each output
file has exactly the requested size. Large sizes require corresponding free
disk space for all generated files.

## Benchmark current streaming engine

After generating fixtures, benchmark streaming XML or CSV record parsing and
DuckDB ingestion. The benchmark hashes both inputs, checks their exact size and
manifest digest, checks persisted record counts, and reports elapsed time,
throughput, batch size, configured memory split, and peak process memory. Its
database, temporary files, and JSON report stay under `.test_data/`.

```powershell
python -m tools.benchmark_large_files --manifest .test_data/csv_100MB_expected.json
python -m tools.benchmark_large_files --manifest .test_data/xml_100MB_expected.json
```

By default, the report is written next to the manifest and DuckDB files are
written under `.test_data/benchmark_storage/`. Use `--max-memory-mb` and
`--batch-size` to record different settings. This step benchmarks parsing and
storage only; `difference_comparison_performed` is false because the record
comparison joins are implemented in a later step. The expected-differences
manifest counts are reported but are not claimed as validated by the
benchmark.

### Initial 100 MB baseline

On the Windows 11 development machine (Python 3.13.17, DuckDB 2.0.1), with a
512 MiB job budget, 256 MiB DuckDB limit, and 1,000-row batches, both formats
passed their SHA-256 and persisted-row-count checks:

| Format | Input processed | Rows ingested | Elapsed | Throughput | Peak process memory |
| --- | ---: | ---: | ---: | ---: | ---: |
| CSV | 200 MB (two 100 MB files) | 195,312 | 195.15 s | 0.98 MiB/s | 261,087,232 bytes |
| XML | 200 MB (two 100 MB files) | 195,312 | 193.92 s | 0.98 MiB/s | 254,369,792 bytes |

This is a baseline for streaming parse and storage, not a performance target or
an end-to-end comparison result. It shows bounded input parsing at this file
size, but does not prove memory behavior at 1 GiB or 2 GiB. A separate 10 MB
CSV batch-size trial measured 23.35 s at 1,000 rows and 22.05 s at 10,000 rows;
the modest throughput improvement did not justify the larger batch's higher
peak process memory, so the default batch size remains 1,000. Re-run on the
target machine and retain each JSON report for comparable measurements.
