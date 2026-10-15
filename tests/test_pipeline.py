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


class TestOptionalDatabaseIntegration(unittest.TestCase):

    def _run_mock_pipeline(self, database_url=None):
        import sys
        from unittest.mock import patch

        sys.path.insert(0, str(REPO_ROOT / "src"))
        from sales_pipeline import pipeline

        import tempfile

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            raw_dir = root / "raw"
            output_dir = root / "processed"
            raw_dir.mkdir()
            output_dir.mkdir()

            def save_output(filename):
                def _save(*args, **kwargs):
                    path = output_dir / filename
                    path.write_text("test\n", encoding="utf-8")
                    return path
                return _save

            orders = pd.DataFrame({"order_id": ["a", "b"]})
            products = pd.DataFrame({"product_id": ["p"]})
            categories = pd.DataFrame({"category": ["c"]})
            customers = pd.DataFrame({"customer": ["u"]})
            monthly = pd.DataFrame({"month": ["2020-01"]})
            status = pd.DataFrame({"status": ["delivered"]})

            database_result = {
                "order_analytics": {
                    "source_rows": 2,
                    "loaded_rows": 2,
                }
            }

            with (
                patch.object(
                    pipeline.validate,
                    "validate_data",
                    return_value=[{"check": "mock"}],
                ),
                patch.object(
                    pipeline.validate,
                    "print_report",
                    return_value=True,
                ),
                patch.object(
                    pipeline.transform,
                    "build_order_analytics",
                    return_value=orders,
                ),
                patch.object(
                    pipeline.transform,
                    "save_order_analytics",
                    side_effect=save_output("order_analytics.csv"),
                ),
                patch.object(
                    pipeline.transform_products,
                    "build_product_analytics",
                    return_value=(products, categories),
                ),
                patch.object(
                    pipeline.transform_products,
                    "save_product_analytics",
                    side_effect=lambda *args: (
                        save_output("product_analytics.csv")(),
                        save_output("category_analytics.csv")(),
                    ),
                ),
                patch.object(
                    pipeline.transform_customers,
                    "build_customer_analytics",
                    return_value=customers,
                ),
                patch.object(
                    pipeline.transform_customers,
                    "save_customer_analytics",
                    side_effect=save_output("customer_analytics.csv"),
                ),
                patch.object(
                    pipeline.transform_sales,
                    "build_sales_analytics",
                    return_value=(monthly, status),
                ),
                patch.object(
                    pipeline.transform_sales,
                    "save_sales_analytics",
                    side_effect=lambda *args: (
                        save_output("monthly_sales_analytics.csv")(),
                        save_output("order_status_summary.csv")(),
                    ),
                ),
                patch.object(
                    pipeline.database,
                    "load_processed_csvs",
                    return_value=database_result,
                ) as database_loader,
            ):
                summary = pipeline.run_pipeline(
                    raw_dir,
                    output_dir,
                    database_url=database_url,
                )

                return summary, database_loader

    def test_csv_only_mode_skips_database_loader(self):
        summary, loader = self._run_mock_pipeline()

        self.assertFalse(summary["database_loaded"])
        self.assertIsNone(summary["database_results"])
        loader.assert_not_called()

    def test_database_mode_calls_loader_and_records_results(self):
        summary, loader = self._run_mock_pipeline(
            database_url="postgresql://test-placeholder"
        )

        self.assertTrue(summary["database_loaded"])
        self.assertIsNotNone(summary["database_results"])
        loader.assert_called_once()

        args = loader.call_args.kwargs
        self.assertEqual(
            args["database_url"],
            "postgresql://test-placeholder",
        )
        self.assertEqual(Path(args["processed_dir"]).name, "processed")
