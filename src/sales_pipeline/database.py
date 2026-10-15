"""Database loading functions for the sales analytics warehouse."""

import psycopg


CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS order_analytics (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    order_status TEXT,
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date DATE,
    item_count INTEGER,
    unique_products INTEGER,
    unique_sellers INTEGER,
    item_revenue NUMERIC(14, 2),
    freight_cost NUMERIC(14, 2),
    payment_total NUMERIC(14, 2),
    payment_records INTEGER,
    payment_types INTEGER,
    delivery_duration_days DOUBLE PRECISION,
    estimated_delivery_duration_days DOUBLE PRECISION,
    delivery_delay_days DOUBLE PRECISION,
    is_delivered BOOLEAN,
    is_late_delivery BOOLEAN
);

CREATE TABLE IF NOT EXISTS product_analytics (
    product_id TEXT PRIMARY KEY,
    product_category_name_english TEXT,
    units_sold INTEGER,
    item_revenue NUMERIC(14, 2),
    freight_cost NUMERIC(14, 2),
    distinct_orders INTEGER,
    distinct_sellers INTEGER
);

CREATE TABLE IF NOT EXISTS category_analytics (
    product_category_name_english TEXT PRIMARY KEY,
    units_sold INTEGER,
    item_revenue NUMERIC(14, 2),
    freight_cost NUMERIC(14, 2),
    distinct_orders INTEGER,
    distinct_products INTEGER,
    distinct_sellers INTEGER
);

CREATE TABLE IF NOT EXISTS customer_analytics (
    customer_unique_id TEXT PRIMARY KEY,
    order_count INTEGER,
    item_revenue NUMERIC(14, 2),
    first_order_date TIMESTAMP,
    last_order_date TIMESTAMP,
    orders_with_revenue INTEGER,
    customer_state TEXT,
    average_order_value NUMERIC(14, 2),
    is_repeat_customer BOOLEAN
);

CREATE TABLE IF NOT EXISTS monthly_sales_analytics (
    order_month TEXT PRIMARY KEY,
    order_count INTEGER,
    item_revenue NUMERIC(14, 2),
    freight_cost NUMERIC(14, 2),
    total_items DOUBLE PRECISION,
    orders_with_revenue INTEGER,
    average_order_value NUMERIC(14, 2)
);

CREATE TABLE IF NOT EXISTS order_status_summary (
    order_status TEXT PRIMARY KEY,
    order_count INTEGER
);
"""


def create_tables(database_url: str) -> None:
    """Create the analytics tables if they do not already exist."""
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            for statement in CREATE_TABLES_SQL.split(";"):
                if statement.strip():
                    cur.execute(statement)


import csv
from io import StringIO
from pathlib import Path

from psycopg import sql


CSV_TABLE_MAP = {
    "order_analytics.csv": "order_analytics",
    "product_analytics.csv": "product_analytics",
    "category_analytics.csv": "category_analytics",
    "customer_analytics.csv": "customer_analytics",
    "monthly_sales_analytics.csv": "monthly_sales_analytics",
    "order_status_summary.csv": "order_status_summary",
}


def load_processed_csvs(
    database_url: str,
    processed_dir: str | Path,
) -> dict[str, dict[str, int]]:
    """Atomically replace warehouse table contents from processed CSVs."""

    processed_dir = Path(processed_dir)

    # Fail before modifying the database if any file is missing.
    missing_files = [
        filename
        for filename in CSV_TABLE_MAP
        if not (processed_dir / filename).is_file()
    ]

    if missing_files:
        raise FileNotFoundError(
            f"Missing processed CSV files: {missing_files}"
        )

    results = {}

    # A single transaction covers all six tables.
    # If any load fails, all truncations and inserts roll back.
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:

            # Read and validate the CSV headers before truncating.
            for filename, table_name in CSV_TABLE_MAP.items():
                with (processed_dir / filename).open(
                    "r", encoding="utf-8-sig", newline=""
                ) as file:
                    reader = csv.reader(file)
                    csv_columns = next(reader, None)

                cur.execute(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = %s
                    ORDER BY ordinal_position
                    """,
                    (table_name,),
                )
                db_columns = [row[0] for row in cur.fetchall()]

                if csv_columns != db_columns:
                    raise ValueError(
                        f"Column mismatch for {filename}. "
                        f"CSV={csv_columns}; DB={db_columns}"
                    )

            # Full refresh: replace all six table contents.
            table_identifiers = [
                sql.Identifier(table)
                for table in CSV_TABLE_MAP.values()
            ]
            cur.execute(
                sql.SQL("TRUNCATE TABLE {}").format(
                    sql.SQL(", ").join(table_identifiers)
                )
            )

            for filename, table_name in CSV_TABLE_MAP.items():
                file_path = processed_dir / filename

                with file_path.open(
                    "r", encoding="utf-8-sig", newline=""
                ) as file:
                    csv_buffer = StringIO(file.read())

                copy_query = sql.SQL(
                    "COPY {} FROM STDIN WITH "
                    "(FORMAT CSV, HEADER TRUE, NULL '')"
                ).format(sql.Identifier(table_name))

                with cur.copy(copy_query) as copy:
                    copy.write(csv_buffer.getvalue())

                cur.execute(
                    sql.SQL("SELECT COUNT(*) FROM {}").format(
                        sql.Identifier(table_name)
                    )
                )
                loaded_rows = cur.fetchone()[0]

                # Count source records independently of the database.
                with file_path.open(
                    "r", encoding="utf-8-sig", newline=""
                ) as file:
                    source_rows = sum(1 for _ in csv.reader(file)) - 1

                if loaded_rows != source_rows:
                    raise RuntimeError(
                        f"Row-count mismatch for {table_name}: "
                        f"CSV={source_rows}, DB={loaded_rows}"
                    )

                results[table_name] = {
                    "source_rows": source_rows,
                    "loaded_rows": loaded_rows,
                }

    return results
