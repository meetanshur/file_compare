"""Bounded-batch DuckDB storage for records and comparison findings."""

from __future__ import annotations

import json
import threading
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import duckdb

from common.comparison_result import ComparisonType, Difference, Severity


class StorageError(RuntimeError):
    """A persistent storage operation failed."""


class RecordSide(StrEnum):
    """Input file a stored record belongs to."""

    FILE_A = "A"
    FILE_B = "B"


@dataclass(frozen=True)
class StoredRecord:
    """A record fetched from persistent storage."""

    record_id: int
    side: RecordSide
    record_key: list[list[Any]] | None
    payload: dict[str, Any]


@dataclass(frozen=True)
class PersistResult:
    """Outcome of persisting a bounded stream of findings."""

    inserted: int
    total_persisted: int
    truncated: bool


def _json_default(value: Any) -> str:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    raise TypeError(f"Value of type {type(value).__name__} is not JSON serializable")


def _serialize_json(value: Any) -> str:
    return json.dumps(
        value,
        default=_json_default,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


class DuckDBStore:
    """Store streamed records and findings in a job-scoped DuckDB database."""

    def __init__(
        self,
        database_path: str | Path,
        *,
        job_id: UUID | None = None,
        max_memory_mb: int = 512,
        temp_directory: str | Path | None = None,
        max_total_differences: int = 1_000_000,
        batch_size: int = 50_000,
    ) -> None:
        if max_total_differences < 0:
            raise ValueError("max_total_differences must be zero or greater")
        if batch_size < 1:
            raise ValueError("batch_size must be greater than zero")
        if max_memory_mb < 1:
            raise ValueError("max_memory_mb must be greater than zero")

        self.database_path = Path(database_path)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self.temp_directory = (
            Path(temp_directory)
            if temp_directory is not None
            else self.database_path.parent / f"{self.database_path.stem}_tmp"
        )
        self.temp_directory.mkdir(parents=True, exist_ok=True)
        self.max_total_differences = max_total_differences
        self.max_memory_mb = max_memory_mb
        self.batch_size = batch_size
        self._lock = threading.RLock()
        self._connection: duckdb.DuckDBPyConnection | None = None

        try:
            self._connection = duckdb.connect(
                str(self.database_path),
                config={
                    "memory_limit": f"{max_memory_mb}MiB",
                    "temp_directory": str(self.temp_directory),
                },
            )
            self._initialize_schema()
            stored_job_id = self._get_metadata("job_id")
            if stored_job_id is None:
                stored_job_id = str(job_id or uuid4())
                self._set_metadata("job_id", stored_job_id)
            elif job_id is not None and stored_job_id != str(job_id):
                raise ValueError("The database already belongs to a different comparison job")
            self.job_id = UUID(stored_job_id)
            self._difference_count = self.count_differences()
            self._truncated = self._get_metadata("truncated") == "true"
        except (duckdb.Error, OSError) as exc:
            self._close_after_initialization_failure()
            raise StorageError(
                "Could not initialize DuckDB storage; check database and temporary-directory "
                "permissions and free disk space."
            ) from exc
        except ValueError:
            self._close_after_initialization_failure()
            raise

    def _initialize_schema(self) -> None:
        connection = self._require_connection()
        connection.execute("CREATE SEQUENCE IF NOT EXISTS record_id_sequence")
        connection.execute("CREATE SEQUENCE IF NOT EXISTS difference_id_sequence")
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS store_metadata (
                key VARCHAR PRIMARY KEY,
                value VARCHAR NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS comparison_records (
                record_id BIGINT PRIMARY KEY DEFAULT nextval('record_id_sequence'),
                side VARCHAR NOT NULL CHECK (side IN ('A', 'B')),
                record_key JSON,
                record_key_text VARCHAR,
                payload JSON NOT NULL,
                comparison_payload JSON
            )
            """
        )
        connection.execute(
            "ALTER TABLE comparison_records ADD COLUMN IF NOT EXISTS record_key_text VARCHAR"
        )
        connection.execute(
            "ALTER TABLE comparison_records ADD COLUMN IF NOT EXISTS comparison_payload JSON"
        )
        connection.execute(
            """
            UPDATE comparison_records
            SET record_key_text = CAST(record_key AS VARCHAR)
            WHERE record_key IS NOT NULL AND record_key_text IS NULL
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS comparison_differences (
                difference_id BIGINT PRIMARY KEY DEFAULT nextval('difference_id_sequence'),
                payload JSON NOT NULL
            )
            """
        )

    def _require_connection(self) -> duckdb.DuckDBPyConnection:
        if self._connection is None:
            raise StorageError("DuckDB storage is closed")
        return self._connection

    def _get_metadata(self, key: str) -> str | None:
        row = (
            self._require_connection()
            .execute("SELECT value FROM store_metadata WHERE key = ?", [key])
            .fetchone()
        )
        return None if row is None else str(row[0])

    def _set_metadata(self, key: str, value: str) -> None:
        self._require_connection().execute(
            """
            INSERT INTO store_metadata (key, value) VALUES (?, ?)
            ON CONFLICT (key) DO UPDATE SET value = excluded.value
            """,
            [key, value],
        )

    def _close_after_initialization_failure(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    @property
    def truncated(self) -> bool:
        """Whether the configured persisted-finding limit stopped the job."""
        return self._truncated

    def insert_records(
        self,
        side: RecordSide,
        records: Iterable[Mapping[str, Any]],
        *,
        key_fields: Sequence[str] = (),
        trim_keys: bool = False,
        key_case_sensitive: bool = True,
        cancellation_check: Callable[[], None] | None = None,
    ) -> int:
        """Persist a record stream in bounded batches and return its row count."""
        if not isinstance(side, RecordSide):
            raise ValueError("side must be a RecordSide value")
        inserted = 0
        batch: list[tuple[str, str | None, str | None, str, str | None]] = []

        with self._lock:
            self._require_connection()
            try:
                for record in records:
                    if cancellation_check is not None:
                        cancellation_check()
                    missing_keys = [field for field in key_fields if field not in record]
                    if missing_keys:
                        raise ValueError(f"Record is missing key fields: {', '.join(missing_keys)}")
                    key = None
                    comparison_record = dict(record)
                    if key_fields:
                        key = []
                        for field in key_fields:
                            value = record[field]
                            if trim_keys or not key_case_sensitive:
                                value = str(value)
                                if trim_keys:
                                    value = value.strip()
                                if not key_case_sensitive:
                                    value = value.casefold()
                            key.append([field, value])
                            comparison_record[field] = value
                    key_json = None if key is None else _serialize_json(key)
                    payload_json = _serialize_json(record)
                    comparison_payload_json = (
                        None if comparison_record == record else _serialize_json(comparison_record)
                    )
                    batch.append(
                        (
                            side.value,
                            key_json,
                            key_json,
                            payload_json,
                            comparison_payload_json,
                        )
                    )
                    if len(batch) >= self.batch_size:
                        self._insert_record_batch(batch)
                        inserted += len(batch)
                        batch.clear()
                if batch:
                    self._insert_record_batch(batch)
                    inserted += len(batch)
            except (duckdb.Error, OSError) as exc:
                raise StorageError(
                    "Could not persist records to DuckDB; check free disk space and storage "
                    "permissions."
                ) from exc
        return inserted

    def iter_record_comparison_differences(
        self,
        *,
        cancellation_check: Callable[[], None] | None = None,
        batch_size: int = 1_000,
    ) -> Iterator[Difference]:
        """Yield SQL-joined differences, including order-insensitive duplicate-group checks."""
        if batch_size < 1:
            raise ValueError("batch_size must be greater than zero")

        with self._lock:
            connection = self._require_connection()
            try:
                connection.execute("DROP TABLE IF EXISTS record_comparison_findings")
                limit_clause = ""
                if self.max_total_differences:
                    remaining = self.max_total_differences - self._difference_count
                    limit_clause = f"LIMIT {max(1, remaining)}"
                connection.execute(
                    f"""
                    CREATE TEMP TABLE record_comparison_findings AS
                    WITH key_counts AS (
                        SELECT side, record_key_text, count(*) AS record_count
                        FROM comparison_records
                        WHERE record_key_text IS NOT NULL
                        GROUP BY side, record_key_text
                    ),
                    duplicate_keys AS (
                        SELECT record_key_text
                        FROM key_counts
                        WHERE record_count > 1
                        GROUP BY record_key_text
                    ),
                    unique_a AS (
                        SELECT record_id, record_key, record_key_text, payload, comparison_payload
                        FROM comparison_records
                        WHERE side = 'A'
                          AND record_key_text IS NOT NULL
                          AND record_key_text NOT IN (SELECT record_key_text FROM duplicate_keys)
                    ),
                    unique_b AS (
                        SELECT record_id, record_key, record_key_text, payload, comparison_payload
                        FROM comparison_records
                        WHERE side = 'B'
                          AND record_key_text IS NOT NULL
                          AND record_key_text NOT IN (SELECT record_key_text FROM duplicate_keys)
                    ),
                    duplicate_findings AS (
                        SELECT
                            'duplicate_key' AS comparison_type,
                            c.record_key_text,
                            min(c.record_key_text)::JSON AS record_key,
                            c.side,
                            c.record_count,
                            NULL::JSON AS value_a,
                            NULL::JSON AS value_b,
                            'duplicate_warning' AS detail
                        FROM key_counts c
                        WHERE c.record_count > 1
                        GROUP BY c.record_key_text, c.side, c.record_count
                    ),
                    duplicate_totals AS (
                        SELECT side, record_key_text, record_count
                        FROM key_counts
                        WHERE record_key_text IN (SELECT record_key_text FROM duplicate_keys)
                    ),
                    duplicate_size_findings AS (
                        SELECT
                            'duplicate_key' AS comparison_type,
                            d.record_key_text,
                            d.record_key_text::JSON AS record_key,
                            'A vs B' AS side,
                            NULL::BIGINT AS record_count,
                            to_json(struct_pack(record_count := coalesce(a.record_count, 0)))
                                AS value_a,
                            to_json(struct_pack(record_count := coalesce(b.record_count, 0)))
                                AS value_b,
                            'group_size' AS detail
                        FROM (
                            SELECT
                                d.record_key_text,
                                min(r.record_key_text)::JSON AS record_key
                            FROM duplicate_keys d
                            JOIN comparison_records r USING (record_key_text)
                            GROUP BY d.record_key_text
                        ) d
                        LEFT JOIN duplicate_totals a
                          ON a.record_key_text = d.record_key_text AND a.side = 'A'
                        LEFT JOIN duplicate_totals b
                          ON b.record_key_text = d.record_key_text AND b.side = 'B'
                        WHERE coalesce(a.record_count, 0) != coalesce(b.record_count, 0)
                    ),
                    duplicate_payload_counts AS (
                        SELECT
                            side,
                            record_key_text,
                            CAST(payload AS VARCHAR) AS payload_text,
                            payload,
                            count(*) AS record_count
                        FROM comparison_records
                        WHERE record_key_text IN (SELECT record_key_text FROM duplicate_keys)
                        GROUP BY side, record_key_text, CAST(payload AS VARCHAR), payload
                    ),
                    duplicate_record_findings AS (
                        SELECT
                            'duplicate_key' AS comparison_type,
                            coalesce(a.record_key_text, b.record_key_text) AS record_key_text,
                            coalesce(a.record_key_text, b.record_key_text)::JSON AS record_key,
                            'A vs B' AS side,
                            NULL::BIGINT AS record_count,
                            CASE
                                WHEN a.record_count IS NULL THEN NULL::JSON
                                ELSE to_json(struct_pack(
                                    record_value := a.payload,
                                    count := a.record_count
                                ))
                            END AS value_a,
                            CASE
                                WHEN b.record_count IS NULL THEN NULL::JSON
                                ELSE to_json(struct_pack(
                                    record_value := b.payload,
                                    count := b.record_count
                                ))
                            END AS value_b,
                            'record_mismatch' AS detail
                        FROM (
                            SELECT * FROM duplicate_payload_counts WHERE side = 'A'
                        ) a
                        FULL OUTER JOIN (
                            SELECT * FROM duplicate_payload_counts WHERE side = 'B'
                        ) b
                          ON a.record_key_text = b.record_key_text
                         AND a.payload_text = b.payload_text
                        WHERE a.record_count IS NULL OR b.record_count IS NULL
                    ),
                    record_findings AS (
                        SELECT
                            CASE
                                WHEN a.record_id IS NULL THEN 'insert'
                                WHEN b.record_id IS NULL THEN 'delete'
                                ELSE 'update'
                            END AS comparison_type,
                            coalesce(a.record_key_text, b.record_key_text) AS record_key_text,
                            coalesce(a.record_key, b.record_key) AS record_key,
                            NULL::VARCHAR AS side,
                            NULL::BIGINT AS record_count,
                            a.payload AS value_a,
                            b.payload AS value_b,
                            'record' AS detail
                        FROM unique_a a
                        FULL OUTER JOIN unique_b b
                          ON a.record_key_text = b.record_key_text
                        WHERE a.record_id IS NULL
                           OR b.record_id IS NULL
                           OR coalesce(a.comparison_payload, a.payload)
                              IS DISTINCT FROM
                              coalesce(b.comparison_payload, b.payload)
                    )
                    SELECT *
                    FROM (
                        SELECT * FROM duplicate_findings
                        UNION ALL
                        SELECT * FROM duplicate_size_findings
                        UNION ALL
                        SELECT * FROM duplicate_record_findings
                        UNION ALL
                        SELECT * FROM record_findings
                    ) all_findings
                    ORDER BY comparison_type, record_key_text, side, detail
                    {limit_clause}
                    """
                )
                offset = 0
                while True:
                    if cancellation_check is not None:
                        cancellation_check()
                    rows = connection.execute(
                        """
                        SELECT
                            comparison_type, record_key, side, record_count,
                            value_a, value_b, detail
                        FROM record_comparison_findings
                        ORDER BY comparison_type, record_key_text, side, detail
                        LIMIT ? OFFSET ?
                        """,
                        [batch_size, offset],
                    ).fetchall()
                    if not rows:
                        break
                    offset += len(rows)
                    for (
                        comparison_type,
                        record_key,
                        side,
                        record_count,
                        value_a,
                        value_b,
                        detail,
                    ) in rows:
                        if cancellation_check is not None:
                            cancellation_check()
                        key = json.loads(record_key) if isinstance(record_key, str) else record_key
                        key_text = _serialize_json(key)
                        if comparison_type == ComparisonType.DUPLICATE_KEY.value:
                            if isinstance(value_a, str):
                                value_a = json.loads(value_a)
                            if isinstance(value_b, str):
                                value_b = json.loads(value_b)
                            if detail == "duplicate_warning":
                                message = (
                                    f"Duplicate record key appears {record_count} times in "
                                    f"file {side}."
                                )
                                severity = Severity.WARNING
                                value_a = {"key": key, "count": record_count}
                                value_b = None
                            elif detail == "group_size":
                                count_a = value_a["record_count"]
                                count_b = value_b["record_count"]
                                message = (
                                    f"Duplicate-key group size differs: file A has {count_a}, "
                                    f"file B has {count_b}."
                                )
                                severity = Severity.ERROR
                            else:
                                message = (
                                    "Duplicate-key record set differs "
                                    "(compared order-insensitively)."
                                )
                                severity = Severity.ERROR
                            yield Difference(
                                comparison_type=ComparisonType.DUPLICATE_KEY,
                                location=f"side={side} key={key_text}",
                                message=message,
                                severity=severity,
                                value_a=value_a,
                                value_b=value_b,
                            )
                            continue
                        category = ComparisonType(comparison_type)
                        if isinstance(value_a, str):
                            value_a = json.loads(value_a)
                        if isinstance(value_b, str):
                            value_b = json.loads(value_b)
                        message = {
                            ComparisonType.INSERT: "Record exists only in file B",
                            ComparisonType.UPDATE: "Record fields differ",
                            ComparisonType.DELETE: "Record exists only in file A",
                        }[category]
                        yield Difference(
                            comparison_type=category,
                            location=f"key={key_text}",
                            message=message,
                            value_a=value_a,
                            value_b=value_b,
                        )
            except duckdb.Error as exc:
                raise StorageError("Could not compare stored records in DuckDB.") from exc
            finally:
                connection.execute("DROP TABLE IF EXISTS record_comparison_findings")

    def _insert_record_batch(
        self, batch: list[tuple[str, str | None, str | None, str, str | None]]
    ) -> None:
        connection = self._require_connection()
        connection.execute("BEGIN TRANSACTION")
        appender = connection.appender(
            "comparison_records",
            columns=[
                "side",
                "record_key",
                "record_key_text",
                "payload",
                "comparison_payload",
            ],
        )
        try:
            appender.append_rows(batch)
            appender.flush()
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise

    def insert_csv_file(
        self,
        side: RecordSide,
        csv_path: str | Path,
        *,
        key_fields: Sequence[str] = (),
    ) -> int:
        """Ingest a CSV file with DuckDB's native streaming CSV reader."""
        if not isinstance(side, RecordSide):
            raise ValueError("side must be a RecordSide value")
        path = Path(csv_path).resolve()
        if not path.is_file():
            raise FileNotFoundError(path)

        with self._lock:
            connection = self._require_connection()
            try:
                describe = connection.execute(
                    """
                    DESCRIBE SELECT * FROM read_csv(
                        ?, header = true, all_varchar = true, auto_detect = true
                    )
                    """,
                    [str(path)],
                )
                columns = [str(row[0]) for row in describe.fetchall()]
                if not columns:
                    raise ValueError(f"{path.name} has no CSV columns")
                missing_keys = [field for field in key_fields if field not in columns]
                if missing_keys:
                    raise ValueError(f"CSV is missing key fields: {', '.join(missing_keys)}")

                def quote_identifier(identifier: str) -> str:
                    return '"' + identifier.replace('"', '""') + '"'

                def quote_literal(value: str) -> str:
                    return "'" + value.replace("'", "''") + "'"

                if key_fields:
                    key_parts = ", ".join(
                        f"json_array({quote_literal(field)}, "
                        f"CAST({quote_identifier(field)} AS VARCHAR))"
                        for field in key_fields
                    )
                    key_json = f"json_array({key_parts})"
                else:
                    key_json = "NULL::JSON"
                payload_parts = ", ".join(
                    f"{quote_identifier(field)} := {quote_identifier(field)}" for field in columns
                )
                payload_json = f"to_json(struct_pack({payload_parts}))"
                force_not_null = "[" + ", ".join(quote_literal(column) for column in columns) + "]"
                query = f"""
                    INSERT INTO comparison_records
                        (side, record_key, record_key_text, payload, comparison_payload)
                    SELECT ?::VARCHAR, {key_json}, CAST({key_json} AS VARCHAR),
                        {payload_json}, NULL::JSON
                    FROM read_csv(
                        ?, header = true, all_varchar = true, auto_detect = true,
                        force_not_null = {force_not_null}
                    )
                """
                before_count = self.count_records(side)
                connection.execute("BEGIN TRANSACTION")
                try:
                    connection.execute(query, [side.value, str(path)])
                    connection.execute("COMMIT")
                except Exception:
                    connection.execute("ROLLBACK")
                    raise
                return self.count_records(side) - before_count
            except (duckdb.Error, OSError) as exc:
                raise StorageError(
                    f"Could not ingest CSV file {path.name} with DuckDB; check its format, "
                    "permissions, and available disk space."
                ) from exc

    def create_record_key_index(self) -> None:
        """Create the record matching index after records have been loaded."""
        with self._lock:
            try:
                self._require_connection().execute(
                    """
                    CREATE INDEX IF NOT EXISTS comparison_records_key_idx
                    ON comparison_records (side, record_key_text)
                    """
                )
            except duckdb.Error as exc:
                raise StorageError("Could not create the record-key index.") from exc

    def count_records(self, side: RecordSide | None = None) -> int:
        """Return the number of records stored, optionally for one input side."""
        with self._lock:
            try:
                if side is None:
                    row = (
                        self._require_connection()
                        .execute("SELECT count(*) FROM comparison_records")
                        .fetchone()
                    )
                else:
                    row = (
                        self._require_connection()
                        .execute(
                            "SELECT count(*) FROM comparison_records WHERE side = ?",
                            [side.value],
                        )
                        .fetchone()
                    )
            except duckdb.Error as exc:
                raise StorageError("Could not count stored records.") from exc
        return int(row[0])

    def iter_records(
        self, side: RecordSide | None = None, *, batch_size: int = 1_000
    ) -> Iterator[StoredRecord]:
        """Yield stored records in bounded batches."""
        if batch_size < 1:
            raise ValueError("batch_size must be greater than zero")

        with self._lock:
            try:
                connection = self._require_connection()
                if side is None:
                    cursor = connection.execute(
                        """
                        SELECT record_id, side, record_key, payload
                        FROM comparison_records ORDER BY record_id
                        """
                    )
                else:
                    cursor = connection.execute(
                        """
                        SELECT record_id, side, record_key, payload
                        FROM comparison_records WHERE side = ? ORDER BY record_id
                        """,
                        [side.value],
                    )
                while rows := cursor.fetchmany(batch_size):
                    for record_id, row_side, record_key, payload in rows:
                        yield StoredRecord(
                            record_id=record_id,
                            side=RecordSide(row_side),
                            record_key=None if record_key is None else json.loads(record_key),
                            payload=json.loads(payload),
                        )
            except duckdb.Error as exc:
                raise StorageError("Could not read stored records.") from exc

    def insert_differences(self, differences: Iterable[Difference]) -> PersistResult:
        """Persist findings in bounded batches until the configured limit is met."""
        inserted = 0
        batch: list[tuple[str]] = []

        with self._lock:
            self._require_connection()
            try:
                for difference in differences:
                    if self._truncated or (
                        self.max_total_differences > 0
                        and self._difference_count >= self.max_total_differences
                    ):
                        self._mark_truncated()
                        break
                    batch.append((_serialize_json(difference.model_dump(mode="json")),))
                    if len(batch) >= self._allowed_batch_size():
                        self._insert_difference_batch(batch)
                        inserted += len(batch)
                        self._difference_count += len(batch)
                        batch.clear()
                        if self._at_difference_limit():
                            self._mark_truncated()
                            break
                if batch:
                    self._insert_difference_batch(batch)
                    inserted += len(batch)
                    self._difference_count += len(batch)
                    if self._at_difference_limit():
                        self._mark_truncated()
            except (duckdb.Error, OSError) as exc:
                raise StorageError(
                    "Could not persist comparison findings to DuckDB; check free disk space "
                    "and storage permissions."
                ) from exc

        return PersistResult(
            inserted=inserted,
            total_persisted=self._difference_count,
            truncated=self._truncated,
        )

    def _allowed_batch_size(self) -> int:
        if self.max_total_differences == 0:
            return self.batch_size
        remaining = self.max_total_differences - self._difference_count
        return min(self.batch_size, remaining)

    def _at_difference_limit(self) -> bool:
        return (
            self.max_total_differences > 0 and self._difference_count >= self.max_total_differences
        )

    def _mark_truncated(self) -> None:
        self._truncated = True
        self._set_metadata("truncated", "true")

    def _insert_difference_batch(self, batch: list[tuple[str]]) -> None:
        connection = self._require_connection()
        connection.execute("BEGIN TRANSACTION")
        try:
            connection.executemany(
                "INSERT INTO comparison_differences (payload) VALUES (?::JSON)", batch
            )
            connection.execute("COMMIT")
        except duckdb.Error:
            connection.execute("ROLLBACK")
            raise

    def count_differences(self, comparison_type: ComparisonType | None = None) -> int:
        """Return persisted finding count, optionally by comparison category."""
        with self._lock:
            try:
                connection = self._require_connection()
                if comparison_type is None:
                    row = connection.execute(
                        "SELECT count(*) FROM comparison_differences"
                    ).fetchone()
                else:
                    row = connection.execute(
                        """
                        SELECT count(*)
                        FROM comparison_differences
                        WHERE json_extract_string(payload, '$.comparison_type') = ?
                        """,
                        [comparison_type.value],
                    ).fetchone()
            except duckdb.Error as exc:
                raise StorageError("Could not count stored findings.") from exc
        return int(row[0])

    def iter_differences(
        self,
        *,
        batch_size: int = 1_000,
        comparison_type: ComparisonType | None = None,
        offset: int = 0,
        limit: int | None = None,
    ) -> Iterator[Difference]:
        """Yield findings in bounded batches, optionally for a page or category."""
        if batch_size < 1:
            raise ValueError("batch_size must be greater than zero")
        if offset < 0 or (limit is not None and limit < 0):
            raise ValueError("offset and limit must be zero or greater")

        with self._lock:
            try:
                connection = self._require_connection()
                query = "SELECT payload FROM comparison_differences"
                parameters: list[Any] = []
                if comparison_type is not None:
                    query += " WHERE json_extract_string(payload, '$.comparison_type') = ?"
                    parameters.append(comparison_type.value)
                query += " ORDER BY difference_id"
                if limit is not None:
                    query += " LIMIT ?"
                    parameters.append(limit)
                if offset:
                    query += " OFFSET ?"
                    parameters.append(offset)
                cursor = connection.execute(query, parameters)
                while rows := cursor.fetchmany(batch_size):
                    for (payload,) in rows:
                        yield Difference.model_validate_json(payload)
            except duckdb.Error as exc:
                raise StorageError("Could not read stored findings.") from exc

    def close(self) -> None:
        """Close the database connection."""
        with self._lock:
            if self._connection is not None:
                try:
                    self._connection.close()
                except duckdb.Error as exc:
                    raise StorageError("Could not close DuckDB storage cleanly.") from exc
                finally:
                    self._connection = None

    def __enter__(self) -> DuckDBStore:
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.close()
