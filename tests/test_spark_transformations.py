"""Unit tests for reusable PySpark sales transformations."""

import unittest

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
)

from sales_pipeline.spark_transformations import (
    build_order_analytics_spark,
    prepare_sales_items_spark,
    build_product_analytics_spark,
    build_category_analytics_spark,
)


class TestSparkTransformations(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("SalesPipelineSparkTests")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_product_and_category_aggregations(self):
        orders_schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("order_status", StringType(), False),
        ])
        items_schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("order_item_id", IntegerType(), False),
            StructField("product_id", StringType(), False),
            StructField("seller_id", StringType(), False),
            StructField("price", DoubleType(), False),
            StructField("freight_value", DoubleType(), False),
        ])
        products_schema = StructType([
            StructField("product_id", StringType(), False),
            StructField("product_category_name", StringType(), True),
        ])
        translations_schema = StructType([
            StructField("product_category_name", StringType(), False),
            StructField("product_category_name_english", StringType(), False),
        ])

        orders = self.spark.createDataFrame([
            ("o1", "delivered"),
            ("o2", "delivered"),
            ("o3", "canceled"),
            ("o4", "unavailable"),
        ], orders_schema)

        items = self.spark.createDataFrame([
            ("o1", 1, "p1", "s1", 10.0, 2.0),
            ("o1", 2, "p1", "s1", 10.0, 3.0),
            ("o2", 1, "p2", "s2", 20.0, 4.0),
            ("o3", 1, "p1", "s1", 99.0, 9.0),
            ("o4", 1, "p2", "s2", 88.0, 8.0),
        ], items_schema)

        products = self.spark.createDataFrame([
            ("p1", "cat_a"),
            ("p2", "cat_untranslated"),
        ], products_schema)

        translations = self.spark.createDataFrame([
            ("cat_a", "category_a"),
        ], translations_schema)

        sales_items = prepare_sales_items_spark(
            orders, items, products, translations
        )

        self.assertEqual(sales_items.count(), 3)

        fallback_count = sales_items.filter(
            sales_items.product_category_name_english
            == "unknown_or_untranslated"
        ).count()
        self.assertEqual(fallback_count, 1)

        products_result = build_product_analytics_spark(sales_items)
        self.assertEqual(products_result.count(), 2)

        p1 = products_result.filter(
            products_result.product_id == "p1"
        ).first()
        self.assertEqual(p1["units_sold"], 2)
        self.assertAlmostEqual(p1["item_revenue"], 20.0, places=2)
        self.assertAlmostEqual(p1["freight_cost"], 5.0, places=2)
        self.assertEqual(p1["distinct_orders"], 1)

        categories_result = build_category_analytics_spark(sales_items)
        self.assertEqual(categories_result.count(), 2)

        category_a = categories_result.filter(
            categories_result.product_category_name_english
            == "category_a"
        ).first()
        self.assertEqual(category_a["units_sold"], 2)
        self.assertAlmostEqual(category_a["item_revenue"], 20.0, places=2)

    def test_order_level_metrics_preserve_one_row_per_order(self):
        orders_schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("order_status", StringType(), False),
            StructField("order_purchase_timestamp", TimestampType(), True),
            StructField("order_delivered_customer_date", TimestampType(), True),
            StructField("order_estimated_delivery_date", TimestampType(), True),
        ])
        items_schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("order_item_id", IntegerType(), False),
            StructField("product_id", StringType(), False),
            StructField("seller_id", StringType(), False),
            StructField("price", DoubleType(), False),
            StructField("freight_value", DoubleType(), False),
        ])
        payments_schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("payment_sequential", IntegerType(), False),
            StructField("payment_type", StringType(), False),
            StructField("payment_value", DoubleType(), False),
        ])
        customers_schema = StructType([
            StructField("customer_id", StringType(), False),
            StructField("customer_unique_id", StringType(), False),
            StructField("customer_city", StringType(), False),
            StructField("customer_state", StringType(), False),
        ])

        from datetime import datetime

        orders = self.spark.createDataFrame([
            (
                "o1", "c1", "delivered",
                datetime(2024, 1, 1),
                datetime(2024, 1, 4),
                datetime(2024, 1, 3),
            ),
            (
                "o2", "c2", "canceled",
                datetime(2024, 1, 2),
                None,
                datetime(2024, 1, 5),
            ),
        ], orders_schema)

        items = self.spark.createDataFrame([
            ("o1", 1, "p1", "s1", 10.0, 2.0),
            ("o1", 2, "p2", "s2", 20.0, 3.0),
        ], items_schema)

        payments = self.spark.createDataFrame([
            ("o1", 1, "credit_card", 30.0),
            ("o1", 2, "voucher", 5.0),
        ], payments_schema)

        customers = self.spark.createDataFrame([
            ("c1", "u1", "city_a", "SP"),
            ("c2", "u2", "city_b", "RJ"),
        ], customers_schema)

        result = build_order_analytics_spark(
            orders, items, payments, customers
        )

        self.assertEqual(result.count(), 2)
        self.assertEqual(result.select("order_id").distinct().count(), 2)

        delivered = result.filter(result.order_id == "o1").first()
        self.assertEqual(delivered["item_count"], 2)
        self.assertAlmostEqual(delivered["item_revenue"], 30.0, places=2)
        self.assertAlmostEqual(delivered["payment_total"], 35.0, places=2)
        self.assertEqual(delivered["payment_records"], 2)
        self.assertEqual(delivered["payment_types"], 2)
        self.assertTrue(delivered["is_delivered"])
        self.assertTrue(delivered["is_late_delivery"])

        canceled = result.filter(result.order_id == "o2").first()
        self.assertEqual(canceled["item_count"], 0)
        self.assertEqual(canceled["payment_records"], 0)
        self.assertFalse(canceled["is_delivered"])
        self.assertFalse(canceled["is_late_delivery"])


if __name__ == "__main__":
    unittest.main()
