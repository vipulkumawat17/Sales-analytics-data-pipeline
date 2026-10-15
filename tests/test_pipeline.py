"""Tests for sales analytics pipeline outputs."""

import unittest
from pathlib import Path
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
PROCESSED_DIR = REPO_ROOT / "data" / "processed"


class TestPipelineOutputs(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.orders = pd.read_csv(
            PROCESSED_DIR / "order_analytics.csv"
        )
        cls.products = pd.read_csv(
            PROCESSED_DIR / "product_analytics.csv"
        )
        cls.categories = pd.read_csv(
            PROCESSED_DIR / "category_analytics.csv"
        )
        cls.customers = pd.read_csv(
            PROCESSED_DIR / "customer_analytics.csv"
        )
        cls.monthly = pd.read_csv(
            PROCESSED_DIR / "monthly_sales_analytics.csv"
        )
        cls.status = pd.read_csv(
            PROCESSED_DIR / "order_status_summary.csv"
        )

    def test_order_ids_are_unique(self):
        self.assertEqual(
            self.orders["order_id"].nunique(),
            len(self.orders)
        )

    def test_product_ids_are_unique(self):
        self.assertTrue(self.products["product_id"].is_unique)

    def test_category_names_are_unique(self):
        self.assertTrue(
            self.categories[
                "product_category_name_english"
            ].is_unique
        )

    def test_customer_ids_are_unique(self):
        self.assertTrue(
            self.customers["customer_unique_id"].is_unique
        )

    def test_customer_repeat_flag(self):
        expected = self.customers["order_count"] > 1
        actual = self.customers["is_repeat_customer"].astype(bool)
        pd.testing.assert_series_equal(
            actual.reset_index(drop=True),
            expected.reset_index(drop=True),
            check_names=False
        )

    def test_product_and_category_units_reconcile(self):
        self.assertEqual(
            self.products["units_sold"].sum(),
            self.categories["units_sold"].sum()
        )

    def test_product_and_category_revenue_reconcile(self):
        self.assertAlmostEqual(
            self.products["item_revenue"].sum(),
            self.categories["item_revenue"].sum(),
            places=2
        )

    def test_order_status_counts_reconcile(self):
        self.assertEqual(
            int(self.status["order_count"].sum()),
            99441
        )

    def test_monthly_order_month_is_unique(self):
        self.assertTrue(self.monthly["order_month"].is_unique)

    def test_expected_outputs_exist(self):
        expected_files = [
            "order_analytics.csv",
            "product_analytics.csv",
            "category_analytics.csv",
            "customer_analytics.csv",
            "monthly_sales_analytics.csv",
            "order_status_summary.csv",
        ]
        for filename in expected_files:
            with self.subTest(filename=filename):
                path = PROCESSED_DIR / filename
                self.assertTrue(path.is_file())
                self.assertGreater(path.stat().st_size, 0)


if __name__ == "__main__":
    unittest.main()
