from pathlib import Path
import pandas as pd


# Explicit mapping to the actual source filenames.
DATASET_FILES = {
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "payments": "olist_order_payments_dataset.csv",
    "reviews": "olist_order_reviews_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}


# Required source columns, based on our profiling results.
DATASET_SCHEMAS = {
    "orders": [
        "order_id", "customer_id", "order_status",
        "order_purchase_timestamp", "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ],
    "order_items": [
        "order_id", "order_item_id", "product_id",
        "seller_id", "shipping_limit_date",
        "price", "freight_value",
    ],
    "customers": [
        "customer_id", "customer_unique_id",
        "customer_zip_code_prefix", "customer_city",
        "customer_state",
    ],
    "products": [
        "product_id", "product_category_name",
        "product_name_lenght", "product_description_lenght",
        "product_photos_qty", "product_weight_g",
        "product_length_cm", "product_height_cm",
        "product_width_cm",
    ],
    "sellers": [
        "seller_id", "seller_zip_code_prefix",
        "seller_city", "seller_state",
    ],
    "payments": [
        "order_id", "payment_sequential", "payment_type",
        "payment_installments", "payment_value",
    ],
    "reviews": [
        "review_id", "order_id", "review_score",
        "review_comment_title", "review_comment_message",
        "review_creation_date", "review_answer_timestamp",
    ],
    "geolocation": [
        "geolocation_zip_code_prefix", "geolocation_lat",
        "geolocation_lng", "geolocation_city",
        "geolocation_state",
    ],
    "category_translation": [
        "product_category_name",
        "product_category_name_english",
    ],
}


def load_csv(data_dir, dataset_name, chunksize=None):
    """
    Load an Olist CSV after validating its required columns.

    Args:
        data_dir: Directory containing the source CSV files.
        dataset_name: Key from DATASET_FILES.
        chunksize: Optional number of rows per chunk.

    Returns:
        A pandas DataFrame, or a pandas TextFileReader if chunked.
    """
    if dataset_name not in DATASET_FILES:
        raise ValueError(
            f"Unknown dataset '{dataset_name}'. "
            f"Available datasets: {list(DATASET_FILES)}"
        )

    path = Path(data_dir) / DATASET_FILES[dataset_name]

    if not path.is_file():
        raise FileNotFoundError(
            f"Dataset file not found: {path}"
        )

    # Read only the header first to verify the schema.
    header = pd.read_csv(path, nrows=0, encoding="utf-8")

    required_columns = DATASET_SCHEMAS[dataset_name]
    missing_columns = [
        column for column in required_columns
        if column not in header.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Schema mismatch in {path.name}. "
            f"Missing required columns: {missing_columns}"
        )

    return pd.read_csv(
        path,
        chunksize=chunksize,
        encoding="utf-8",
        low_memory=False,
    )
