"""Persistent Phase 4 tests for Parquet storage and dimensional modeling."""

import unittest
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"
ANALYTICS_DIR = DATA_DIR / "gold" / "analytics"
DIMENSIONAL_DIR = DATA_DIR / "gold" / "dimensional"


class TestPhase4StorageAndModeling(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        required_paths = [
            ANALYTICS_DIR / "monthly_sales_analytics",
            DIMENSIONAL_DIR / "dim_customer",
            DIMENSIONAL_DIR / "dim_product",
            DIMENSIONAL_DIR / "dim_seller",
            DIMENSIONAL_DIR / "dim_order",
            DIMENSIONAL_DIR / "fact_sales",
        ]

        missing = [str(p) for p in required_paths if not p.exists()]
        if missing:
            raise unittest.SkipTest(
                "Phase 4 Parquet data unavailable: " + ", ".join(missing)
            )

        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("SalesPipelinePhase4Tests")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

        cls.analytics = {}
        for name in [
            "order_analytics",
            "product_analytics",
            "category_analytics",
            "customer_analytics",
            "monthly_sales_analytics",
            "order_status_summary",
        ]:
            path = ANALYTICS_DIR / name
            if path.exists():
                cls.analytics[name] = cls.spark.read.parquet(str(path))

        cls.tables = {}
        for name in [
            "dim_customer",
            "dim_product",
            "dim_seller",
            "dim_order",
            "fact_sales",
        ]:
            cls.tables[name] = cls.spark.read.parquet(
                str(DIMENSIONAL_DIR / name)
            )

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "spark"):
            cls.spark.stop()

    def test_gold_analytics_row_counts(self):
        expected = {
            "order_analytics": 99441,
            "product_analytics": 32729,
            "category_analytics": 72,
            "customer_analytics": 94990,
            "monthly_sales_analytics": 24,
            "order_status_summary": 8,
        }

        for name, expected_count in expected.items():
            with self.subTest(table=name):
                self.assertIn(name, self.analytics)
                self.assertEqual(
                    self.analytics[name].count(), expected_count
                )

    def test_dimensional_table_row_counts(self):
        expected = {
            "dim_customer": 99441,
            "dim_product": 32951,
            "dim_seller": 3095,
            "dim_order": 99441,
            "fact_sales": 112650,
        }

        for name, expected_count in expected.items():
            with self.subTest(table=name):
                self.assertEqual(self.tables[name].count(), expected_count)

    def test_dimension_business_keys_are_unique_and_non_null(self):
        keys = {
            "dim_customer": "customer_id",
            "dim_product": "product_id",
            "dim_seller": "seller_id",
            "dim_order": "order_id",
        }

        for table_name, key in keys.items():
            with self.subTest(table=table_name):
                df = self.tables[table_name]
                self.assertEqual(
                    df.filter(F.col(key).isNull()).count(), 0
                )
                duplicates = (
                    df.groupBy(key)
                    .count()
                    .filter(F.col("count") > 1)
                    .count()
                )
                self.assertEqual(duplicates, 0)

    def test_fact_business_key_is_unique_and_required_keys_non_null(self):
        fact = self.tables["fact_sales"]
        required = [
            "order_id",
            "order_item_id",
            "dim_order_key",
            "dim_customer_key",
            "dim_product_key",
            "dim_seller_key",
        ]

        null_condition = None
        for column in required:
            condition = F.col(column).isNull()
            null_condition = (
                condition if null_condition is None
                else null_condition | condition
            )

        self.assertEqual(fact.filter(null_condition).count(), 0)

        duplicate_keys = (
            fact.groupBy("order_id", "order_item_id")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        self.assertEqual(duplicate_keys, 0)

    def test_fact_foreign_keys_resolve_to_dimensions(self):
        fact = self.tables["fact_sales"]
        relationships = [
            ("dim_order_key", "dim_order"),
            ("dim_customer_key", "dim_customer"),
            ("dim_product_key", "dim_product"),
            ("dim_seller_key", "dim_seller"),
        ]

        for fact_key, dimension_name in relationships:
            with self.subTest(fact_key=fact_key):
                dimension_keys = self.tables[dimension_name].select(
                    fact_key
                ).distinct()

                unmatched = (
                    fact.select(fact_key)
                    .distinct()
                    .join(dimension_keys, on=fact_key, how="left_anti")
                )
                self.assertEqual(unmatched.count(), 0)

    def test_eligible_fact_sales_reconcile_with_monthly_analytics(self):
        fact = self.tables["fact_sales"]
        orders = self.tables["dim_order"]
        monthly = self.analytics["monthly_sales_analytics"]

        eligible = (
            fact.join(
                orders.select("order_id", "order_status"),
                on="order_id",
                how="inner",
            )
            .filter(~F.col("order_status").isin("canceled", "unavailable"))
        )

        fact_metrics = eligible.agg(
            F.count(F.lit(1)).alias("item_rows"),
            F.sum("price").alias("revenue"),
            F.sum("freight_value").alias("freight"),
        ).first()

        monthly_metrics = monthly.agg(
            F.sum("total_items").alias("item_rows"),
            F.sum("item_revenue").alias("revenue"),
            F.sum("freight_cost").alias("freight"),
        ).first()

        self.assertEqual(int(fact_metrics["item_rows"]), 112101)
        self.assertEqual(int(monthly_metrics["item_rows"]), 112101)

        self.assertAlmostEqual(
            float(fact_metrics["revenue"]),
            float(monthly_metrics["revenue"]),
            delta=0.01,
        )
        self.assertAlmostEqual(
            float(fact_metrics["freight"]),
            float(monthly_metrics["freight"]),
            delta=0.01,
        )


if __name__ == "__main__":
    unittest.main()
