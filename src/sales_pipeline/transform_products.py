"""Product and category analytics transformations."""

from pathlib import Path

import pandas as pd

from .extract import load_csv


EXCLUDED_SALES_STATUSES = {"canceled", "unavailable"}


def build_product_analytics(data_dir):
    """Build product- and category-level sales analytics."""
    data_dir = Path(data_dir)

    items = load_csv(data_dir, "order_items")
    products = load_csv(data_dir, "products")
    translations = load_csv(data_dir, "category_translation")
    orders = load_csv(data_dir, "orders")

    # Join order status to each item before calculating sales metrics.
    sales_items = items.merge(
        orders[["order_id", "order_status"]],
        on="order_id",
        how="left",
        validate="many_to_one",
    )

    # Exclude cancelled and unavailable orders from sales metrics.
    sales_items = sales_items[
        ~sales_items["order_status"].isin(EXCLUDED_SALES_STATUSES)
    ].copy()

    # Add product attributes and translated category names.
    sales_items = sales_items.merge(
        products,
        on="product_id",
        how="left",
        validate="many_to_one",
    )

    sales_items = sales_items.merge(
        translations,
        on="product_category_name",
        how="left",
        validate="many_to_one",
    )

    # Keep unmapped categories visible instead of dropping them.
    sales_items["product_category_name_english"] = (
        sales_items["product_category_name_english"]
        .fillna("unknown_or_untranslated")
    )

    product_analytics = (
        sales_items.groupby(
            ["product_id", "product_category_name_english"],
            as_index=False,
            dropna=False,
        )
        .agg(
            units_sold=("order_item_id", "count"),
            item_revenue=("price", "sum"),
            freight_cost=("freight_value", "sum"),
            distinct_orders=("order_id", "nunique"),
            distinct_sellers=("seller_id", "nunique"),
        )
        .sort_values("item_revenue", ascending=False)
        .reset_index(drop=True)
    )

    category_analytics = (
        sales_items.groupby(
            "product_category_name_english",
            as_index=False,
            dropna=False,
        )
        .agg(
            units_sold=("order_item_id", "count"),
            item_revenue=("price", "sum"),
            freight_cost=("freight_value", "sum"),
            distinct_orders=("order_id", "nunique"),
            distinct_products=("product_id", "nunique"),
            distinct_sellers=("seller_id", "nunique"),
        )
        .sort_values("item_revenue", ascending=False)
        .reset_index(drop=True)
    )

    return product_analytics, category_analytics


def save_product_analytics(product_analytics, category_analytics, output_dir):
    """Save product- and category-level analytics CSV files."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    product_path = output_dir / "product_analytics.csv"
    category_path = output_dir / "category_analytics.csv"

    product_analytics.to_csv(product_path, index=False)
    category_analytics.to_csv(category_path, index=False)

    return product_path, category_path
