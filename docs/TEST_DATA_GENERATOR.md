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
`--batch-size` to record different settings. CSV uses DuckDB's native
`read_csv`/insert-select path. XML uses `lxml.etree.iterparse` with processed
elements and previous siblings cleared, then persists 50,000-row Appender
batches by default. A record-key index is created only after both files are
loaded. The benchmark reports the Python parser-only pass, hash time, ingestion
time, XML parse/storage estimates, index time, and peak process memory. For
CSV, native parsing and insertion occur together inside DuckDB and are reported
as one time. This step benchmarks parsing and storage only;
`difference_comparison_performed` is false because record comparison joins
are implemented in a later step. Manifest difference counts are reported but
not claimed as validated by the benchmark.

### 100 MB ingestion profile and optimized baseline

The initial 1,000-row `executemany` implementation took about 194–195 seconds
to ingest two 100 MB files. A separate parser-only pass took 0.44–0.71 seconds
per file, while SHA-256 hashing took about 0.1 seconds per file. This isolated
DuckDB's per-row insert/commit overhead as the bottleneck, not parsing or
hashing.

After switching to DuckDB's native CSV reader and 50,000-row XML Appender
batches, on the same Windows 11 development machine (Python 3.13.17, DuckDB
2.0.1), with a 512 MiB job budget and 256 MiB DuckDB limit:

| Format | Input processed | Rows ingested | Elapsed | Throughput | Peak process memory |
| --- | ---: | ---: | ---: | ---: | ---: |
| CSV | 200 MB (two 100 MB files) | 195,312 | 3.97 s | 48.04 MiB/s | 350,027,776 bytes (334 MiB) |
| XML | 200 MB (two 100 MB files) | 195,312 | 7.44 s | 25.63 MiB/s | 320,417,792 bytes (306 MiB) |

Phase breakdown for the optimized runs:

- **CSV:** DuckDB native parse plus insert took 2.14 s total; the diagnostic
  Python CSV parser-only pass took 1.43 s, SHA-256 took 0.20 s, and index
  creation took 0.11 s. Native CSV parsing and SQL insertion happen together
  and are not separated by DuckDB.
- **XML:** During ingestion, `lxml` parsing took 1.58 s and the estimated
  serialization/storage remainder took 4.06 s; the diagnostic parser-only pass
  took 1.42 s, SHA-256 took 0.19 s, and post-load index creation took 0.11 s.

Both formats met the requested throughput targets and stayed below the 512 MiB
process-memory budget on this machine. These are machine-specific ingestion
measurements, not end-to-end comparison results or proof of behavior at 1 GiB
and 2 GiB. Re-run on the target machine and retain each JSON report for
comparable measurements.
