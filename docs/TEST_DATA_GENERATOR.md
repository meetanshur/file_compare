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
