"""Batch tracking and lifecycle management for incremental processing."""

import hashlib
import json
import logging

import psycopg


BATCH_TABLE = "public.pipeline_batch_runs"

logger = logging.getLogger(__name__)


def make_batch_id(source_name: str, batch_key: str) -> str:
    """Return a deterministic ID for a logical source batch."""
    if not isinstance(source_name, str) or not source_name.strip():
        raise ValueError("source_name cannot be empty")
    if not isinstance(batch_key, str) or not batch_key.strip():
        raise ValueError("batch_key cannot be empty")

    identity = json.dumps(
        {
            "source": source_name.strip(),
            "batch_key": batch_key.strip(),
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()
    return f"batch_{digest[:32]}"


def register_batch(
    database_url: str,
    batch_id: str,
    batch_type: str = "incremental",
) -> str:
    """Register a batch or retry a failed batch.

    Returns 'started' for a new or retried batch and 'skipped' for
    an already successful batch. Concurrent running batches are rejected.
    """
    if not isinstance(batch_id, str) or not batch_id.strip():
        raise ValueError("batch_id cannot be empty")
    if batch_type not in {"incremental", "full_refresh"}:
        raise ValueError("Unsupported batch_type")

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.pipeline_batch_runs
                    (batch_id, batch_type, status, attempt_number)
                VALUES (%s, %s, 'running', 1)
                ON CONFLICT (batch_id) DO NOTHING
                RETURNING batch_id
                """,
                (batch_id, batch_type),
            )

            if cur.fetchone() is not None:
                logger.info(
                    "Batch registered: batch_id=%s type=%s attempt=1",
                    batch_id,
                    batch_type,
                )
                return "started"

            cur.execute(
                """
                SELECT status, batch_type, attempt_number
                FROM public.pipeline_batch_runs
                WHERE batch_id = %s
                FOR UPDATE
                """,
                (batch_id,),
            )
            row = cur.fetchone()

            if row is None:
                raise RuntimeError("Batch registration state unavailable")

            status, existing_type, attempt_number = row

            if existing_type != batch_type:
                raise ValueError(
                    "Batch ID already exists with a different batch_type"
                )

            if status == "succeeded":
                logger.info(
                    "Batch skipped because it already succeeded: batch_id=%s",
                    batch_id,
                )
                return "skipped"

            if status == "running":
                raise RuntimeError(
                    "Batch is already running; concurrent execution rejected"
                )

            if status == "failed":
                cur.execute(
                    """
                    UPDATE public.pipeline_batch_runs
                    SET status = 'running',
                        started_at = NOW(),
                        finished_at = NULL,
                        error_message = NULL,
                        attempt_number = %s
                    WHERE batch_id = %s
                    """,
                    (attempt_number + 1, batch_id),
                )
                logger.info(
                    "Batch retry started: batch_id=%s attempt=%s",
                    batch_id,
                    attempt_number + 1,
                )
                return "started"

            raise RuntimeError(f"Unexpected batch status: {status}")


def mark_batch_succeeded(
    database_url: str,
    batch_id: str,
    source_rows: int = 0,
    inserted_rows: int = 0,
    updated_rows: int = 0,
    rejected_rows: int = 0,
) -> None:
    """Mark a running batch successful and record its row counts."""
    counts = (source_rows, inserted_rows, updated_rows, rejected_rows)
    if any(not isinstance(value, int) or value < 0 for value in counts):
        raise ValueError("Row counts must be non-negative integers")

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE public.pipeline_batch_runs
                SET status = 'succeeded',
                    finished_at = NOW(),
                    source_rows = %s,
                    inserted_rows = %s,
                    updated_rows = %s,
                    rejected_rows = %s,
                    error_message = NULL
                WHERE batch_id = %s
                  AND status = 'running'
                RETURNING batch_id
                """,
                (
                    source_rows,
                    inserted_rows,
                    updated_rows,
                    rejected_rows,
                    batch_id,
                ),
            )
            if cur.fetchone() is None:
                raise RuntimeError(
                    "Batch not found or is not in running state"
                )

    logger.info(
        "Batch succeeded: batch_id=%s source_rows=%s "
        "inserted_rows=%s updated_rows=%s rejected_rows=%s",
        batch_id, source_rows, inserted_rows, updated_rows, rejected_rows,
    )


def mark_batch_failed(
    database_url: str,
    batch_id: str,
    error_message: str,
) -> None:
    """Mark a running batch failed with a diagnostic message."""
    if not isinstance(error_message, str) or not error_message.strip():
        raise ValueError("An error message is required")

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE public.pipeline_batch_runs
                SET status = 'failed',
                    finished_at = NOW(),
                    error_message = %s
                WHERE batch_id = %s
                  AND status = 'running'
                RETURNING batch_id
                """,
                (error_message[:4000], batch_id),
            )
            if cur.fetchone() is None:
                raise RuntimeError(
                    "Batch not found or is not in running state"
                )

    logger.error(
        "Batch failed: batch_id=%s error=%s",
        batch_id, error_message[:500],
    )

def _validate_order_fields(record: dict) -> str | None:
    """Return a validation error for order status or purchase timestamp."""
    import datetime

    status = record["order_status"]
    if not isinstance(status, str) or not status.strip():
        return "Invalid order_status: expected a non-empty string"

    timestamp = record["order_purchase_timestamp"]

    # Missing timestamps remain permitted by the current pipeline contract.
    if timestamp is None:
        return None

    if isinstance(timestamp, datetime.datetime):
        return None

    if isinstance(timestamp, str):
        value = timestamp.strip()
        if not value:
            return "Invalid order_purchase_timestamp: blank timestamp"

        try:
            datetime.datetime.fromisoformat(
                value.replace("Z", "+00:00")
            )
        except ValueError:
            return (
                "Invalid order_purchase_timestamp: "
                "expected an ISO-formatted timestamp"
            )
        return None

    return (
        "Invalid order_purchase_timestamp: expected datetime, "
        "ISO-formatted string, or None"
    )


def write_incremental_orders(
    database_url: str,
    batch_id: str,
    records: list[dict],
    *,
    source_rows: int | None = None,
    rejected_rows: int = 0,
    _mark_failed_on_error: bool = True,
) -> dict[str, int]:
    """Atomically upsert orders and record batch success.

    If validation or database processing fails, attempt to mark
    the registered batch as failed after the write transaction rolls back.
    """
    try:
        if source_rows is not None and (
            not isinstance(source_rows, int)
            or isinstance(source_rows, bool)
            or source_rows < 0
        ):
            raise ValueError("source_rows must be a non-negative integer")

        if (
            not isinstance(rejected_rows, int)
            or isinstance(rejected_rows, bool)
            or rejected_rows < 0
        ):
            raise ValueError("rejected_rows must be a non-negative integer")

        required_fields = {
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
        }

        if not isinstance(records, list):
            raise TypeError("records must be a list of dictionaries")

        source_rows_effective = (
            len(records) if source_rows is None else source_rows
        )

        seen_ids = set()

        for index, record in enumerate(records):
            if not isinstance(record, dict):
                raise TypeError(
                    f"Record {index} must be a dictionary"
                )

            missing = required_fields - record.keys()
            if missing:
                raise ValueError(
                    f"Record {index} is missing fields: "
                    f"{sorted(missing)}"
                )

            order_id = record["order_id"]
            customer_id = record["customer_id"]

            if not isinstance(order_id, str) or not order_id.strip():
                raise ValueError(
                    f"Record {index} has an invalid order_id"
                )

            if (
                not isinstance(customer_id, str)
                or not customer_id.strip()
            ):
                raise ValueError(
                    f"Record {index} has an invalid customer_id"
                )

            field_error = _validate_order_fields(record)
            if field_error:
                raise ValueError(f"Record {index}: {field_error}")

            if order_id in seen_ids:
                raise ValueError(
                    f"Duplicate order_id within input batch: {order_id}"
                )

            seen_ids.add(order_id)

        inserted_rows = 0
        updated_rows = 0

        # Any exception inside this context rolls back the transaction.
        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT status, batch_type
                    FROM public.pipeline_batch_runs
                    WHERE batch_id = %s
                    FOR UPDATE
                    """,
                    (batch_id,),
                )
                batch = cur.fetchone()

                if batch is None:
                    raise RuntimeError("Batch is not registered")

                if batch[0] != "running":
                    raise RuntimeError(
                        f"Batch must be running, found status: {batch[0]}"
                    )

                if batch[1] != "incremental":
                    raise RuntimeError(
                        "Incremental writer requires an incremental batch"
                    )

                for record in records:
                    cur.execute(
                        """
                        INSERT INTO public.phase5_incremental_orders (
                            order_id,
                            customer_id,
                            order_status,
                            order_purchase_timestamp,
                            source_batch_id,
                            updated_at
                        )
                        VALUES (%s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (order_id) DO UPDATE SET
                            customer_id = EXCLUDED.customer_id,
                            order_status = EXCLUDED.order_status,
                            order_purchase_timestamp =
                                EXCLUDED.order_purchase_timestamp,
                            source_batch_id = EXCLUDED.source_batch_id,
                            updated_at = NOW()
                        RETURNING (xmax = 0) AS was_inserted
                        """,
                        (
                            record["order_id"],
                            record["customer_id"],
                            record["order_status"],
                            record["order_purchase_timestamp"],
                            batch_id,
                        ),
                    )

                    if cur.fetchone()[0]:
                        inserted_rows += 1
                    else:
                        updated_rows += 1

                cur.execute(
                    """
                    UPDATE public.pipeline_batch_runs
                    SET status = 'succeeded',
                        finished_at = NOW(),
                        source_rows = %s,
                        inserted_rows = %s,
                        updated_rows = %s,
                        rejected_rows = %s,
                        error_message = NULL
                    WHERE batch_id = %s
                      AND status = 'running'
                    RETURNING batch_id
                    """,
                    (
                        source_rows_effective,
                        inserted_rows,
                        updated_rows,
                        rejected_rows,
                        batch_id,
                    ),
                )

                if cur.fetchone() is None:
                    raise RuntimeError(
                        "Could not mark batch successful"
                    )

        # Reaching here means the database transaction committed.
        logger.info(
            "Incremental write committed: batch_id=%s "
            "source_rows=%s inserted_rows=%s updated_rows=%s "
            "rejected_rows=%s",
            batch_id,
            source_rows_effective,
            inserted_rows,
            updated_rows,
            rejected_rows,
        )
        return {
            "source_rows": source_rows_effective,
            "inserted_rows": inserted_rows,
            "updated_rows": updated_rows,
            "rejected_rows": rejected_rows,
        }

    except Exception as exc:
        logger.exception(
            "Incremental write failed for batch %s", batch_id
        )
        if _mark_failed_on_error:
            try:
                mark_batch_failed(
                    database_url,
                    batch_id,
                    f"{type(exc).__name__}: {exc}",
                )
            except Exception:
                pass
        raise


def validate_and_quarantine_orders(
    database_url: str,
    batch_id: str,
    records: list[dict],
) -> dict:
    """Split order records into valid and rejected records.

    Rejected records are stored in the Phase 5 quarantine table.
    The batch must already be registered.
    """
    import json

    if not isinstance(records, list):
        raise TypeError("records must be a list of dictionaries")

    required_fields = {
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
    }

    valid_records = []
    rejected_records = []
    seen_ids = set()

    for index, record in enumerate(records):
        reason = None

        if not isinstance(record, dict):
            reason = "Record must be a dictionary"
        else:
            missing = required_fields - record.keys()
            if missing:
                reason = f"Missing required fields: {sorted(missing)}"
            elif (
                not isinstance(record["order_id"], str)
                or not record["order_id"].strip()
            ):
                reason = "Invalid order_id"
            elif (
                not isinstance(record["customer_id"], str)
                or not record["customer_id"].strip()
            ):
                reason = "Invalid customer_id"
            elif (field_error := _validate_order_fields(record)) is not None:
                reason = field_error
            elif record["order_id"] in seen_ids:
                reason = f"Duplicate order_id within batch: {record['order_id']}"

        if reason:
            payload = record if isinstance(record, dict) else {
                "_invalid_record_type": type(record).__name__,
                "_value": repr(record),
            }
            rejected_records.append({
                "record_index": index,
                "record_payload": payload,
                "rejection_reason": reason,
            })
        else:
            seen_ids.add(record["order_id"])
            valid_records.append(record)

    # Persist quarantine records before returning valid records.
    # The uniqueness constraint makes repeated quarantine attempts safe
    # for the same batch and source record index.
    if rejected_records:
        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT status
                    FROM public.pipeline_batch_runs
                    WHERE batch_id = %s
                    FOR UPDATE
                    """,
                    (batch_id,),
                )
                batch = cur.fetchone()

                if batch is None or batch[0] != "running":
                    raise RuntimeError(
                        "Quarantine requires a registered running batch"
                    )

                for rejected in rejected_records:
                    cur.execute(
                        """
                        INSERT INTO public.phase5_rejected_records (
                            batch_id,
                            record_index,
                            record_payload,
                            rejection_reason
                        )
                        VALUES (%s, %s, %s::jsonb, %s)
                        ON CONFLICT (batch_id, record_index) DO NOTHING
                        """,
                        (
                            batch_id,
                            rejected["record_index"],
                            json.dumps(
                                rejected["record_payload"],
                                default=str,
                            ),
                            rejected["rejection_reason"],
                        ),
                    )

        logger.warning(
            "Order records quarantined: batch_id=%s rejected_rows=%s",
            batch_id,
            len(rejected_records),
        )

    logger.info(
        "Order validation completed: batch_id=%s source_rows=%s "
        "valid_rows=%s rejected_rows=%s",
        batch_id,
        len(records),
        len(valid_records),
        len(rejected_records),
    )

    return {
        "valid_records": valid_records,
        "rejected_records": rejected_records,
        "source_rows": len(records),
        "valid_rows": len(valid_records),
        "rejected_rows": len(rejected_records),
    }


def retry_transient_database_operation(
    operation,
    *,
    max_attempts: int = 3,
    initial_delay_seconds: float = 1.0,
    max_delay_seconds: float = 8.0,
):
    """Retry an operation on transient PostgreSQL errors only.

    Uses exponential backoff. The operation must be safe to retry.
    """
    import random
    import time
    import psycopg

    if not callable(operation):
        raise TypeError("operation must be callable")
    if not isinstance(max_attempts, int) or isinstance(max_attempts, bool):
        raise ValueError("max_attempts must be an integer")
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")
    if initial_delay_seconds < 0 or max_delay_seconds < 0:
        raise ValueError("Retry delays cannot be negative")

    transient_errors = (
        psycopg.OperationalError,
        psycopg.errors.SerializationFailure,
        psycopg.errors.DeadlockDetected,
        psycopg.errors.LockNotAvailable,
    )

    for attempt in range(1, max_attempts + 1):
        try:
            return operation()
        except transient_errors as exc:
            if attempt == max_attempts:
                logger.error(
                    "Database operation exhausted retries: attempts=%s error_type=%s",
                    attempt,
                    type(exc).__name__,
                )
                raise

            delay = min(
                initial_delay_seconds * (2 ** (attempt - 1)),
                max_delay_seconds,
            )
            # Small jitter helps avoid synchronized retries.
            delay = delay * random.uniform(0.8, 1.2)

            logger.warning(
                "Transient database error: attempt=%s/%s error_type=%s "
                "retry_delay_seconds=%.2f",
                attempt,
                max_attempts,
                type(exc).__name__,
                delay,
            )
            time.sleep(delay)


def write_incremental_orders_with_retry(
    database_url: str,
    batch_id: str,
    records: list[dict],
    *,
    source_rows: int | None = None,
    rejected_rows: int = 0,
    max_attempts: int = 3,
    initial_delay_seconds: float = 1.0,
    max_delay_seconds: float = 8.0,
) -> dict[str, int]:
    """Retry transient write errors and mark failure after retries are exhausted."""
    try:
        return retry_transient_database_operation(
            lambda: write_incremental_orders(
                database_url,
                batch_id,
                records,
                source_rows=source_rows,
                rejected_rows=rejected_rows,
                _mark_failed_on_error=False,
            ),
            max_attempts=max_attempts,
            initial_delay_seconds=initial_delay_seconds,
            max_delay_seconds=max_delay_seconds,
        )
    except Exception as exc:
        try:
            mark_batch_failed(
                database_url,
                batch_id,
                f"{type(exc).__name__}: {exc}",
            )
        except Exception:
            logger.exception(
                "Could not mark batch %s as failed after retries", batch_id
            )
        raise

def process_incremental_orders(
    database_url: str,
    batch_id: str,
    records: list[dict],
    *,
    max_attempts: int = 3,
    initial_delay_seconds: float = 1.0,
    max_delay_seconds: float = 8.0,
) -> dict:
    """Register, validate, quarantine, and incrementally write an order batch.

    A previously successful batch is skipped. Invalid records are quarantined,
    while valid records are written transactionally with bounded retries.
    """
    registration = register_batch(
        database_url,
        batch_id,
        batch_type="incremental",
    )

    if registration == "skipped":
        return {
            "status": "skipped",
            "batch_id": batch_id,
            "source_rows": 0,
            "inserted_rows": 0,
            "updated_rows": 0,
            "rejected_rows": 0,
        }

    try:
        validation = validate_and_quarantine_orders(
            database_url,
            batch_id,
            records,
        )
    except Exception as exc:
        try:
            mark_batch_failed(
                database_url,
                batch_id,
                f"{type(exc).__name__}: {exc}",
            )
        except Exception:
            logger.exception(
                "Could not mark batch %s as failed after validation "
                "or quarantine error",
                batch_id,
            )
        raise

    result = write_incremental_orders_with_retry(
        database_url,
        batch_id,
        validation["valid_records"],
        source_rows=validation["source_rows"],
        rejected_rows=validation["rejected_rows"],
        max_attempts=max_attempts,
        initial_delay_seconds=initial_delay_seconds,
        max_delay_seconds=max_delay_seconds,
    )

    result["status"] = "succeeded"
    result["batch_id"] = batch_id
    return result
