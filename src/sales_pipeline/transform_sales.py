"""Sales performance analytics transformations."""

from pathlib import Path
import pandas as pd
from .extract import load_csv

EXCLUDED_SALES_STATUSES = {"canceled", "unavailable"}


def build_sales_analytics(data_dir):
    data_dir = Path(data_dir)

    orders = load_csv(data_dir, "orders")
    items = load_csv(data_dir, "order_items")

    orders["order_purchase_timestamp"] = pd.to_datetime(
        orders["order_purchase_timestamp"],
        errors="coerce"
    )

    # Aggregate item revenue once per order to avoid join multiplication.
    order_revenue = (
        items.groupby("order_id", as_index=False)
        .agg(
            item_revenue=("price", "sum"),
            freight_cost=("freight_value", "sum"),
            item_count=("order_item_id", "count"),
        )
    )

    orders = orders.merge(
        order_revenue,
        on="order_id",
        how="left",
        validate="one_to_one"
    )

    # Monthly KPIs use eligible orders only.
    eligible_orders = orders[
        ~orders["order_status"].isin(EXCLUDED_SALES_STATUSES)
    ].copy()

    eligible_orders["order_month"] = (
        eligible_orders["order_purchase_timestamp"].dt.to_period("M").astype(str)
    )

    monthly_sales = (
        eligible_orders.dropna(subset=["order_purchase_timestamp"])
        .groupby("order_month", as_index=False)
        .agg(
            order_count=("order_id", "nunique"),
            item_revenue=("item_revenue", "sum"),
            freight_cost=("freight_cost", "sum"),
            total_items=("item_count", "sum"),
            orders_with_revenue=("item_revenue", "count"),
        )
    )

    monthly_sales["average_order_value"] = (
        monthly_sales["item_revenue"]
        / monthly_sales["orders_with_revenue"].replace(0, pd.NA)
    )

    monthly_sales = monthly_sales.sort_values("order_month").reset_index(drop=True)

    # Keep the complete status distribution separately for operational analysis.
    status_summary = (
        orders.groupby("order_status", as_index=False, dropna=False)
        .agg(order_count=("order_id", "nunique"))
        .sort_values("order_count", ascending=False)
        .reset_index(drop=True)
    )

    return monthly_sales, status_summary


def save_sales_analytics(monthly_sales, status_summary, output_dir):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    monthly_path = output_dir / "monthly_sales_analytics.csv"
    status_path = output_dir / "order_status_summary.csv"

    monthly_sales.to_csv(monthly_path, index=False)
    status_summary.to_csv(status_path, index=False)

    return monthly_path, status_path
