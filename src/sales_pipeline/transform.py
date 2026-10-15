"""Transform raw Olist data into analytics-ready datasets."""

from pathlib import Path

import pandas as pd

from .extract import load_csv


def build_order_analytics(data_dir):
    """
    Build one row per order.

    Revenue is calculated from order items; payment totals are aggregated
    independently to avoid multiplying values during joins.
    """
    data_dir = Path(data_dir)

    orders = load_csv(data_dir, "orders")
    items = load_csv(data_dir, "order_items")
    payments = load_csv(data_dir, "payments")

    # Parse timestamps; invalid or missing values remain NaT.
    date_columns = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]

    for column in date_columns:
        orders[column] = pd.to_datetime(orders[column], errors="coerce")

    # Aggregate item-level data to one row per order.
    item_metrics = (
        items.groupby("order_id", as_index=False)
        .agg(
            item_count=("order_item_id", "count"),
            unique_products=("product_id", "nunique"),
            unique_sellers=("seller_id", "nunique"),
            item_revenue=("price", "sum"),
            freight_cost=("freight_value", "sum"),
        )
    )

    # Aggregate payment-level data independently.
    payment_metrics = (
        payments.groupby("order_id", as_index=False)
        .agg(
            payment_total=("payment_value", "sum"),
            payment_records=("payment_sequential", "count"),
            payment_types=("payment_type", "nunique"),
        )
    )

    # Both aggregate tables have at most one row per order.
    analytics = orders.merge(
        item_metrics, on="order_id", how="left", validate="one_to_one"
    )
    analytics = analytics.merge(
        payment_metrics, on="order_id", how="left", validate="one_to_one"
    )

    # Orders without item or payment records remain in the output.
    count_columns = [
        "item_count",
        "unique_products",
        "unique_sellers",
        "payment_records",
        "payment_types",
    ]
    analytics[count_columns] = analytics[count_columns].fillna(0).astype("int64")

    # Keep missing financial totals as missing when no source records exist.
    # This avoids silently treating absent records as confirmed zero amounts.

    analytics["delivery_duration_days"] = (
        analytics["order_delivered_customer_date"]
        - analytics["order_purchase_timestamp"]
    ).dt.total_seconds() / 86400

    analytics["estimated_delivery_duration_days"] = (
        analytics["order_estimated_delivery_date"]
        - analytics["order_purchase_timestamp"]
    ).dt.total_seconds() / 86400

    analytics["delivery_delay_days"] = (
        analytics["order_delivered_customer_date"]
        - analytics["order_estimated_delivery_date"]
    ).dt.total_seconds() / 86400

    analytics["is_delivered"] = (
        analytics["order_status"].eq("delivered")
    )

    analytics["is_late_delivery"] = (
        analytics["is_delivered"]
        & analytics["delivery_delay_days"].gt(0)
    )

    return analytics


def save_order_analytics(analytics, output_dir):
    """Save order analytics as a CSV file and return its path."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / "order_analytics.csv"
    analytics.to_csv(output_path, index=False)

    return output_path
