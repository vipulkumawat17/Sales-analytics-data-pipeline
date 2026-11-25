"""
Cloud pipeline orchestration for the Sales Analytics Data Engineering Pipeline.

Reads Silver datasets from Cloudflare R2, generates selected Gold analytics
using PySpark, and writes the outputs to dedicated Phase 7 R2 prefixes.
"""

from pathlib import Path
from typing import Any

from sales_pipeline import r2_client
from sales_pipeline import spark_transformations as spark_tf


SILVER_DATASETS = (
    "orders",
    "order_items",
    "payments",
    "customers",
    "products",
    "category_translation",
)

GOLD_PREFIXES = {
    "order_analytics": "phase7/gold/analytics/order_analytics/",
    "product_analytics": "phase7/gold/analytics/product_analytics/",
    "category_analytics": "phase7/gold/analytics/category_analytics/",
}


def run_cloud_pipeline(
    spark: Any,
    local_dir: str | Path,
    client: Any | None = None,
    bucket_name: str | None = None,
    write_mode: str = "error",
    gold_prefixes: dict | None = None,
) -> dict:
    """
    Execute the Silver-to-Gold cloud pipeline using R2 object storage.

    The Spark session and R2 client can be injected for testing.
    Existing Phase 4 Gold objects are never targeted by this function.
    """

    if spark is None:
        raise ValueError("A valid Spark session is required.")

    if write_mode not in ("error", "overwrite"):
        raise ValueError("write_mode must be 'error' or 'overwrite'.")

    local_dir = Path(local_dir)
    local_dir.mkdir(parents=True, exist_ok=True)

    destinations = (
        GOLD_PREFIXES if gold_prefixes is None else gold_prefixes
    )

    if set(destinations) != set(GOLD_PREFIXES):
        raise ValueError(
            "gold_prefixes must contain exactly the configured Gold datasets."
        )

    if any(
        not isinstance(prefix, str) or not prefix.strip()
        for prefix in destinations.values()
    ):
        raise ValueError("Every Gold destination prefix must be non-empty.")

    if client is None:
        client = r2_client.create_r2_client_from_colab()

    if bucket_name is None:
        bucket_name = r2_client.get_bucket_name_from_colab()

    r2_client.verify_bucket(client, bucket_name)

    silver = {}

    for dataset_name in SILVER_DATASETS:
        prefix = f"silver/{dataset_name}/"

        silver[dataset_name] = (
            r2_client.read_parquet_prefix_from_r2(
                spark=spark,
                client=client,
                bucket_name=bucket_name,
                prefix=prefix,
                local_dir=local_dir,
            )
        )

    order_gold = spark_tf.build_order_analytics_spark(
        orders=silver["orders"],
        order_items=silver["order_items"],
        payments=silver["payments"],
        customers=silver["customers"],
    )

    sales_items = spark_tf.prepare_sales_items_spark(
        orders=silver["orders"],
        order_items=silver["order_items"],
        products=silver["products"],
        category_translations=silver["category_translation"],
    )

    product_gold = spark_tf.build_product_analytics_spark(sales_items)
    category_gold = spark_tf.build_category_analytics_spark(sales_items)

    gold_outputs = {
        "order_analytics": order_gold,
        "product_analytics": product_gold,
        "category_analytics": category_gold,
    }

    upload_results = {}

    for dataset_name, dataframe in gold_outputs.items():
        upload_results[dataset_name] = (
            r2_client.write_spark_parquet_to_r2(
                dataframe=dataframe,
                client=client,
                bucket_name=bucket_name,
                prefix=destinations[dataset_name],
                local_dir=local_dir,
                mode=write_mode,
            )
        )

    return {
        "status": "SUCCESS",
        "bucket": bucket_name,
        "silver_datasets_read": list(SILVER_DATASETS),
        "gold_datasets_written": {
            name: {
                "row_count": dataframe.count(),
                "columns": dataframe.columns,
                "destination_prefix": destinations[name],
                "uploaded_file_count": upload_results[name][
                    "uploaded_file_count"
                ],
                "total_size_bytes": upload_results[name][
                    "total_size_bytes"
                ],
                "verification": upload_results[name]["verification"],
            }
            for name, dataframe in gold_outputs.items()
        },
    }
