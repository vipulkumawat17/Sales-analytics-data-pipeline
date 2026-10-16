"""Reusable PySpark transformations for sales analytics."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


EXCLUDED_SALES_STATUSES = ("canceled", "unavailable")


def build_order_analytics_spark(
    orders: DataFrame,
    order_items: DataFrame,
    payments: DataFrame,
    customers: DataFrame,
) -> DataFrame:
    """Build one row per order with item, payment, customer, and delivery metrics."""

    item_metrics = order_items.groupBy("order_id").agg(
        F.count("order_item_id").alias("item_count"),
        F.countDistinct("product_id").alias("unique_products"),
        F.countDistinct("seller_id").alias("unique_sellers"),
        F.sum("price").alias("item_revenue"),
        F.sum("freight_value").alias("freight_cost"),
    )

    payment_metrics = payments.groupBy("order_id").agg(
        F.sum("payment_value").alias("payment_total"),
        F.count("payment_sequential").alias("payment_records"),
        F.countDistinct("payment_type").alias("payment_types"),
    )

    customer_attributes = customers.select(
        "customer_id",
        "customer_unique_id",
        "customer_city",
        "customer_state",
    )

    result = (
        orders
        .join(item_metrics, on="order_id", how="left")
        .join(payment_metrics, on="order_id", how="left")
        .join(customer_attributes, on="customer_id", how="left")
    )

    for column in [
        "item_count",
        "unique_products",
        "unique_sellers",
        "payment_records",
        "payment_types",
    ]:
        result = result.withColumn(
            column,
            F.coalesce(F.col(column), F.lit(0)).cast("long"),
        )

    result = (
        result
        .withColumn(
            "delivery_duration_days",
            (
                F.col("order_delivered_customer_date").cast("long")
                - F.col("order_purchase_timestamp").cast("long")
            ) / F.lit(86400.0),
        )
        .withColumn(
            "estimated_delivery_duration_days",
            (
                F.col("order_estimated_delivery_date").cast("long")
                - F.col("order_purchase_timestamp").cast("long")
            ) / F.lit(86400.0),
        )
        .withColumn(
            "delivery_delay_days",
            (
                F.col("order_delivered_customer_date").cast("long")
                - F.col("order_estimated_delivery_date").cast("long")
            ) / F.lit(86400.0),
        )
        .withColumn(
            "is_delivered",
            F.col("order_status") == F.lit("delivered"),
        )
        .withColumn(
            "is_late_delivery",
            F.when(
                F.col("order_status") == F.lit("delivered"),
                F.coalesce(
                    F.col("delivery_delay_days") > F.lit(0.0),
                    F.lit(False),
                ),
            ).otherwise(F.lit(False)),
        )
    )

    return result


def prepare_sales_items_spark(
    orders: DataFrame,
    order_items: DataFrame,
    products: DataFrame,
    category_translations: DataFrame,
) -> DataFrame:
    """Join eligible order items to products and translated categories."""

    eligible_orders = (
        orders
        .filter(~F.col("order_status").isin(*EXCLUDED_SALES_STATUSES))
        .select("order_id", "order_status")
    )

    translated_categories = (
        category_translations
        .select(
            "product_category_name",
            "product_category_name_english",
        )
    )

    return (
        order_items
        .join(eligible_orders, on="order_id", how="inner")
        .join(
            products.select("product_id", "product_category_name"),
            on="product_id",
            how="left",
        )
        .join(
            translated_categories,
            on="product_category_name",
            how="left",
        )
        .withColumn(
            "product_category_name_english",
            F.coalesce(
                F.col("product_category_name_english"),
                F.lit("unknown_or_untranslated"),
            ),
        )
    )


def build_product_analytics_spark(sales_items: DataFrame) -> DataFrame:
    """Aggregate sales metrics by product and English category."""

    return sales_items.groupBy(
        "product_id",
        "product_category_name_english",
    ).agg(
        F.count("order_item_id").alias("units_sold"),
        F.sum("price").alias("item_revenue"),
        F.sum("freight_value").alias("freight_cost"),
        F.countDistinct("order_id").alias("distinct_orders"),
        F.countDistinct("seller_id").alias("distinct_sellers"),
    )


def build_category_analytics_spark(sales_items: DataFrame) -> DataFrame:
    """Aggregate sales metrics by English category."""

    return sales_items.groupBy(
        "product_category_name_english"
    ).agg(
        F.count("order_item_id").alias("units_sold"),
        F.sum("price").alias("item_revenue"),
        F.sum("freight_value").alias("freight_cost"),
        F.countDistinct("order_id").alias("distinct_orders"),
        F.countDistinct("product_id").alias("distinct_products"),
        F.countDistinct("seller_id").alias("distinct_sellers"),
    )
