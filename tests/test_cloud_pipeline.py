from contextlib import ExitStack
import tempfile
import unittest
from unittest.mock import Mock, patch

from sales_pipeline import cloud_pipeline


class FakeDataFrame:
    def __init__(self, rows, columns):
        self.rows = rows
        self.columns = columns

    def count(self):
        return self.rows


class TestCloudPipeline(unittest.TestCase):

    def setUp(self):
        self.spark = Mock(name="spark")
        self.client = Mock(name="r2_client")
        self.bucket = "sales-analytics-pipeline"
        self.local_dir = tempfile.mkdtemp()

    def test_successful_cloud_pipeline(self):
        silver_frames = {
            name: FakeDataFrame(100, ["id"])
            for name in cloud_pipeline.SILVER_DATASETS
        }

        order_gold = FakeDataFrame(99441, ["order_id", "item_revenue"])
        product_gold = FakeDataFrame(32729, ["product_id", "item_revenue"])
        category_gold = FakeDataFrame(
            72, ["product_category_name_english", "item_revenue"]
        )
        sales_items = FakeDataFrame(112650, ["order_id", "product_id"])

        upload_result = {
            "uploaded_file_count": 2,
            "total_size_bytes": 12345,
            "verification": "PASS",
        }

        with (
            patch.object(
                cloud_pipeline.r2_client,
                "verify_bucket"
            ) as verify_bucket,
            patch.object(
                cloud_pipeline.r2_client,
                "read_parquet_prefix_from_r2",
                side_effect=lambda **kwargs: silver_frames[
                    kwargs["prefix"].split("/")[1]
                ],
            ) as read_silver,
            patch.object(
                cloud_pipeline.spark_tf,
                "build_order_analytics_spark",
                return_value=order_gold,
            ) as build_orders,
            patch.object(
                cloud_pipeline.spark_tf,
                "prepare_sales_items_spark",
                return_value=sales_items,
            ) as prepare_sales,
            patch.object(
                cloud_pipeline.spark_tf,
                "build_product_analytics_spark",
                return_value=product_gold,
            ) as build_products,
            patch.object(
                cloud_pipeline.spark_tf,
                "build_category_analytics_spark",
                return_value=category_gold,
            ) as build_categories,
            patch.object(
                cloud_pipeline.r2_client,
                "write_spark_parquet_to_r2",
                return_value=upload_result,
            ) as write_gold,
        ):
            result = cloud_pipeline.run_cloud_pipeline(
                spark=self.spark,
                local_dir=self.local_dir,
                client=self.client,
                bucket_name=self.bucket,
                write_mode="error",
            )

        verify_bucket.assert_called_once_with(self.client, self.bucket)
        self.assertEqual(read_silver.call_count, 6)

        build_orders.assert_called_once()
        prepare_sales.assert_called_once()
        build_products.assert_called_once_with(sales_items)
        build_categories.assert_called_once_with(sales_items)

        self.assertEqual(write_gold.call_count, 3)
        self.assertEqual(result["status"], "SUCCESS")
        self.assertEqual(result["bucket"], self.bucket)
        self.assertEqual(len(result["silver_datasets_read"]), 6)
        self.assertEqual(len(result["gold_datasets_written"]), 3)

        self.assertEqual(
            result["gold_datasets_written"]["order_analytics"]["row_count"],
            99441,
        )
        self.assertEqual(
            result["gold_datasets_written"]["product_analytics"]["row_count"],
            32729,
        )
        self.assertEqual(
            result["gold_datasets_written"]["category_analytics"]["row_count"],
            72,
        )

        self.assertEqual(
            result["gold_datasets_written"]["order_analytics"][
                "verification"
            ],
            "PASS",
        )

    def test_missing_spark_session_rejected(self):
        with self.assertRaises(ValueError):
            cloud_pipeline.run_cloud_pipeline(
                spark=None,
                local_dir=self.local_dir,
                client=self.client,
                bucket_name=self.bucket,
            )

    def test_invalid_write_mode_rejected(self):
        with self.assertRaises(ValueError):
            cloud_pipeline.run_cloud_pipeline(
                spark=self.spark,
                local_dir=self.local_dir,
                client=self.client,
                bucket_name=self.bucket,
                write_mode="invalid",
            )


if __name__ == "__main__":
    unittest.main()


class TestCloudPipelineFailureHandling(unittest.TestCase):

    def setUp(self):
        self.spark = Mock(name="spark")
        self.client = Mock(name="r2_client")
        self.bucket = "sales-analytics-pipeline"
        self.local_dir = tempfile.mkdtemp()

    def run_with_mocks(self, failure_stage=None):
        silver_frames = {
            name: FakeDataFrame(100, ["id"])
            for name in cloud_pipeline.SILVER_DATASETS
        }

        order_gold = FakeDataFrame(99441, ["order_id", "item_revenue"])
        product_gold = FakeDataFrame(32729, ["product_id", "item_revenue"])
        category_gold = FakeDataFrame(
            72, ["product_category_name_english", "item_revenue"]
        )
        sales_items = FakeDataFrame(112650, ["order_id", "product_id"])

        upload_result = {
            "uploaded_file_count": 2,
            "total_size_bytes": 12345,
            "verification": "PASS",
        }

        with ExitStack() as stack:
            verify_bucket = stack.enter_context(
                patch.object(cloud_pipeline.r2_client, "verify_bucket")
            )

            read_silver = stack.enter_context(
                patch.object(
                    cloud_pipeline.r2_client,
                    "read_parquet_prefix_from_r2",
                    side_effect=lambda **kwargs: silver_frames[
                        kwargs["prefix"].split("/")[1]
                    ],
                )
            )

            build_orders = stack.enter_context(
                patch.object(
                    cloud_pipeline.spark_tf,
                    "build_order_analytics_spark",
                    return_value=order_gold,
                )
            )

            prepare_sales = stack.enter_context(
                patch.object(
                    cloud_pipeline.spark_tf,
                    "prepare_sales_items_spark",
                    return_value=sales_items,
                )
            )

            build_products = stack.enter_context(
                patch.object(
                    cloud_pipeline.spark_tf,
                    "build_product_analytics_spark",
                    return_value=product_gold,
                )
            )

            build_categories = stack.enter_context(
                patch.object(
                    cloud_pipeline.spark_tf,
                    "build_category_analytics_spark",
                    return_value=category_gold,
                )
            )

            write_gold = stack.enter_context(
                patch.object(
                    cloud_pipeline.r2_client,
                    "write_spark_parquet_to_r2",
                    return_value=upload_result,
                )
            )

            if failure_stage == "bucket":
                verify_bucket.side_effect = RuntimeError(
                    "Simulated R2 bucket verification failure"
                )

            elif failure_stage == "silver":
                read_silver.side_effect = FileNotFoundError(
                    "Simulated missing Silver dataset"
                )

            elif failure_stage == "upload":
                write_gold.side_effect = RuntimeError(
                    "Simulated R2 upload failure"
                )

            elif failure_stage == "existing_prefix":
                write_gold.side_effect = FileExistsError(
                    "Destination prefix already contains objects"
                )

            result = cloud_pipeline.run_cloud_pipeline(
                spark=self.spark,
                local_dir=self.local_dir,
                client=self.client,
                bucket_name=self.bucket,
                write_mode="error",
            )

        return result

    def test_bucket_verification_failure(self):
        with self.assertRaisesRegex(
            RuntimeError, "bucket verification failure"
        ):
            self.run_with_mocks("bucket")

    def test_missing_silver_dataset(self):
        with self.assertRaisesRegex(
            FileNotFoundError, "missing Silver dataset"
        ):
            self.run_with_mocks("silver")

    def test_gold_upload_failure(self):
        with self.assertRaisesRegex(
            RuntimeError, "upload failure"
        ):
            self.run_with_mocks("upload")

    def test_existing_gold_prefix_rejected(self):
        with self.assertRaisesRegex(
            FileExistsError, "already contains objects"
        ):
            self.run_with_mocks("existing_prefix")

    def test_invalid_gold_prefix_configuration(self):
        invalid_prefixes = {
            "order_analytics": "phase7-test/orders/",
            "product_analytics": "phase7-test/products/",
        }

        with self.assertRaisesRegex(
            ValueError, "exactly the configured Gold datasets"
        ):
            cloud_pipeline.run_cloud_pipeline(
                spark=self.spark,
                local_dir=self.local_dir,
                client=self.client,
                bucket_name=self.bucket,
                gold_prefixes=invalid_prefixes,
            )
