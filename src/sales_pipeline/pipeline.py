"""Orchestrate the sales analytics data pipeline."""

from pathlib import Path
from datetime import datetime, timezone

from . import validate
from . import transform
from . import transform_products
from . import transform_customers
from . import transform_sales
from . import database


def run_pipeline(data_dir, output_dir, database_url=None):
    data_dir = Path(data_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    started_at = datetime.now(timezone.utc)

    print("=" * 64)
    print("SALES ANALYTICS DATA PIPELINE")
    print("=" * 64)

    # Step 1: Validate source data.
    print("\n[1/5] Validating source data...")
    validation_results = validate.validate_data(data_dir)
    validation_passed = validate.print_report(validation_results)

    if not validation_passed:
        raise RuntimeError(
            "Pipeline stopped: one or more data-quality checks failed."
        )

    # Step 2: Order analytics.
    print("\n[2/5] Building order analytics...")
    order_analytics = transform.build_order_analytics(data_dir)
    order_path = transform.save_order_analytics(
        order_analytics, output_dir
    )

    # Step 3: Product and category analytics.
    print("\n[3/5] Building product and category analytics...")
    product_analytics, category_analytics = (
        transform_products.build_product_analytics(data_dir)
    )
    product_path, category_path = (
        transform_products.save_product_analytics(
            product_analytics, category_analytics, output_dir
        )
    )

    # Step 4: Customer analytics.
    print("\n[4/5] Building customer analytics...")
    customer_analytics = (
        transform_customers.build_customer_analytics(data_dir)
    )
    customer_path = transform_customers.save_customer_analytics(
        customer_analytics, output_dir
    )

    # Step 5: Monthly sales and order-status summary.
    print("\n[5/5] Building sales performance analytics...")
    monthly_sales, status_summary = (
        transform_sales.build_sales_analytics(data_dir)
    )
    monthly_path, status_path = (
        transform_sales.save_sales_analytics(
            monthly_sales, status_summary, output_dir
        )
    )

    outputs = [
        order_path,
        product_path,
        category_path,
        customer_path,
        monthly_path,
        status_path,
    ]

    # Verify that every expected output exists and is non-empty.
    for path in outputs:
        if not path.is_file() or path.stat().st_size == 0:
            raise RuntimeError(
                f"Pipeline output is missing or empty: {path}"
            )

    database_results = None

    if database_url is not None:
        print("\n[6/6] Loading analytics into PostgreSQL...")
        database_results = database.load_processed_csvs(
            database_url=database_url,
            processed_dir=output_dir,
        )
        print("PostgreSQL loading completed successfully.")

    finished_at = datetime.now(timezone.utc)
    duration = (finished_at - started_at).total_seconds()

    summary = {
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "duration_seconds": round(duration, 2),
        "validation_checks": len(validation_results),
        "order_rows": len(order_analytics),
        "product_rows": len(product_analytics),
        "category_rows": len(category_analytics),
        "customer_rows": len(customer_analytics),
        "monthly_rows": len(monthly_sales),
        "status_rows": len(status_summary),
        "outputs": [str(path) for path in outputs],
        "database_loaded": database_results is not None,
        "database_results": database_results,
    }

    print("\n" + "=" * 64)
    print("PIPELINE COMPLETED SUCCESSFULLY")
    print("=" * 64)
    print(f"Duration: {summary['duration_seconds']} seconds")
    print(f"Validation checks: {summary['validation_checks']}")
    print(f"Order rows: {summary['order_rows']}")
    print(f"Product rows: {summary['product_rows']}")
    print(f"Category rows: {summary['category_rows']}")
    print(f"Customer rows: {summary['customer_rows']}")
    print(f"Monthly records: {summary['monthly_rows']}")
    print(f"Order status records: {summary['status_rows']}")

    print("\nGenerated outputs:")
    for path in outputs:
        print(f"- {path} ({path.stat().st_size:,} bytes)")

    return summary
