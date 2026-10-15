"""Customer-level analytics transformations."""

from pathlib import Path
import pandas as pd
from .extract import load_csv

EXCLUDED_SALES_STATUSES = {"canceled", "unavailable"}



def build_customer_analytics(data_dir):
    data_dir = Path(data_dir)

    customers = load_csv(data_dir, "customers")
    orders = load_csv(data_dir, "orders")
    items = load_csv(data_dir, "order_items")

    eligible_orders = orders[
        ~orders["order_status"].isin(EXCLUDED_SALES_STATUSES)
    ].copy()

    eligible_orders["order_purchase_timestamp"] = pd.to_datetime(
        eligible_orders["order_purchase_timestamp"],
        errors="coerce"
    )

    # Calculate item revenue at order level before joining.
    order_revenue = (
        items.groupby("order_id", as_index=False)
        .agg(item_revenue=("price", "sum"))
    )

    eligible_orders = eligible_orders.merge(
        order_revenue,
        on="order_id",
        how="left",
        validate="one_to_one"
    )

    # Attach customer identity and state to each order.
    customer_orders = eligible_orders.merge(
        customers[
            ["customer_id", "customer_unique_id", "customer_state"]
        ],
        on="customer_id",
        how="left",
        validate="many_to_one"
    )

    # Aggregate metrics across all states for each customer.
    metrics = (
        customer_orders.groupby(
            "customer_unique_id",
            as_index=False,
            dropna=False
        )
        .agg(
            order_count=("order_id", "nunique"),
            item_revenue=("item_revenue", "sum"),
            first_order_date=("order_purchase_timestamp", "min"),
            last_order_date=("order_purchase_timestamp", "max"),
            orders_with_revenue=("item_revenue", "count"),
        )
    )

    # Obtain the state from the most recent order.
    latest_orders = (
        customer_orders
        .sort_values(
            ["order_purchase_timestamp", "order_id"],
            na_position="first"
        )
        .drop_duplicates("customer_unique_id", keep="last")
        [["customer_unique_id", "customer_state"]]
    )

    customer_analytics = metrics.merge(
        latest_orders,
        on="customer_unique_id",
        how="left",
        validate="one_to_one"
    )

    customer_analytics["average_order_value"] = (
        customer_analytics["item_revenue"]
        / customer_analytics["orders_with_revenue"].replace(0, pd.NA)
    )

    customer_analytics["is_repeat_customer"] = (
        customer_analytics["order_count"] > 1
    )

    customer_analytics = customer_analytics.sort_values(
        "item_revenue",
        ascending=False
    ).reset_index(drop=True)

    return customer_analytics



def save_customer_analytics(customer_analytics, output_dir):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / "customer_analytics.csv"
    customer_analytics.to_csv(output_path, index=False)

    return output_path
