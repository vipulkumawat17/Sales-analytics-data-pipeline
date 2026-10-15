"""Data quality validation for the Olist sales analytics pipeline."""

from pathlib import Path

import pandas as pd

from .extract import load_csv


def _record(results, check_name, passed, details):
    """Append one validation result."""
    results.append({
        "check": check_name,
        "status": "PASS" if passed else "FAIL",
        "details": str(details),
    })


def _check_not_null(df, column, label, results):
    missing = int(df[column].isna().sum())
    _record(
        results,
        f"{label}: {column} has no nulls",
        missing == 0,
        f"Missing values: {missing}",
    )


def _check_unique(df, columns, label, results):
    duplicates = int(df.duplicated(subset=columns).sum())
    _record(
        results,
        f"{label}: {', '.join(columns)} is unique",
        duplicates == 0,
        f"Duplicate rows for key: {duplicates}",
    )


def _check_foreign_key(
    child_df, child_column, parent_df, parent_column, label, results
):
    child_values = child_df[child_column].dropna()
    parent_values = parent_df[parent_column].dropna()
    unmatched = int((~child_values.isin(parent_values)).sum())

    _record(
        results,
        label,
        unmatched == 0,
        f"Unmatched non-null values: {unmatched}",
    )


def _check_non_negative(df, column, label, results):
    values = pd.to_numeric(df[column], errors="coerce")
    invalid = int((values < 0).sum())
    unparseable = int((values.isna() & df[column].notna()).sum())

    _record(
        results,
        f"{label}: {column} is non-negative",
        invalid == 0 and unparseable == 0,
        f"Negative values: {invalid}; unparseable values: {unparseable}",
    )


def validate_data(data_dir):
    """Run core data-quality checks and return a list of results."""
    data_dir = Path(data_dir)
    results = []

    # Load only the tables needed for these checks.
    customers = load_csv(data_dir, "customers")
    orders = load_csv(data_dir, "orders")
    items = load_csv(data_dir, "order_items")
    products = load_csv(data_dir, "products")
    sellers = load_csv(data_dir, "sellers")
    payments = load_csv(data_dir, "payments")
    reviews = load_csv(data_dir, "reviews")

    # 1. Primary keys and composite key.
    _check_not_null(customers, "customer_id", "Customers", results)
    _check_unique(customers, ["customer_id"], "Customers", results)

    _check_not_null(orders, "order_id", "Orders", results)
    _check_unique(orders, ["order_id"], "Orders", results)

    _check_not_null(items, "order_id", "Order items", results)
    _check_not_null(items, "order_item_id", "Order items", results)
    _check_unique(
        items,
        ["order_id", "order_item_id"],
        "Order items",
        results,
    )

    _check_not_null(products, "product_id", "Products", results)
    _check_unique(products, ["product_id"], "Products", results)

    _check_not_null(sellers, "seller_id", "Sellers", results)
    _check_unique(sellers, ["seller_id"], "Sellers", results)

    # 2. Required relationship fields.
    _check_not_null(orders, "customer_id", "Orders", results)
    _check_not_null(items, "product_id", "Order items", results)
    _check_not_null(items, "seller_id", "Order items", results)
    _check_not_null(payments, "order_id", "Payments", results)
    _check_not_null(reviews, "order_id", "Reviews", results)

    # 3. Referential integrity.
    _check_foreign_key(
        orders, "customer_id", customers, "customer_id",
        "Orders reference existing customers", results,
    )
    _check_foreign_key(
        items, "product_id", products, "product_id",
        "Order items reference existing products", results,
    )
    _check_foreign_key(
        items, "seller_id", sellers, "seller_id",
        "Order items reference existing sellers", results,
    )
    _check_foreign_key(
        items, "order_id", orders, "order_id",
        "Order items reference existing orders", results,
    )
    _check_foreign_key(
        payments, "order_id", orders, "order_id",
        "Payments reference existing orders", results,
    )
    _check_foreign_key(
        reviews, "order_id", orders, "order_id",
        "Reviews reference existing orders", results,
    )

    # 4. Numeric validity.
    for column in ["price", "freight_value"]:
        _check_non_negative(items, column, "Order items", results)

    _check_non_negative(payments, "payment_value", "Payments", results)
    _check_non_negative(
        payments, "payment_installments", "Payments", results
    )

    # 5. Review score must be an integer from 1 through 5.
    scores = pd.to_numeric(reviews["review_score"], errors="coerce")
    invalid_scores = int(
        (
            scores.isna()
            | (scores < 1)
            | (scores > 5)
            | (scores % 1 != 0)
        ).sum()
    )
    _record(
        results,
        "Reviews: scores are integers from 1 to 5",
        invalid_scores == 0,
        f"Invalid scores: {invalid_scores}",
    )

    return results


def print_report(results):
    """Print a readable validation report and summary."""
    print("=" * 72)
    print("SALES ANALYTICS PIPELINE — DATA QUALITY REPORT")
    print("=" * 72)

    for result in results:
        print(
            f"[{result['status']}] {result['check']}\n"
            f"        {result['details']}"
        )

    passed = sum(result["status"] == "PASS" for result in results)
    failed = sum(result["status"] == "FAIL" for result in results)

    print("-" * 72)
    print(f"Total checks: {len(results)}")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    print("=" * 72)

    return failed == 0
