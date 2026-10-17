"""Unit tests for incremental batch identity and validation."""

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from sales_pipeline.incremental import make_batch_id


class TestBatchIdentity(unittest.TestCase):

    def test_same_identity_produces_same_id(self):
        first = make_batch_id("olist_orders", "batch-001")
        second = make_batch_id("olist_orders", "batch-001")
        self.assertEqual(first, second)

    def test_different_batch_keys_produce_different_ids(self):
        first = make_batch_id("olist_orders", "batch-001")
        second = make_batch_id("olist_orders", "batch-002")
        self.assertNotEqual(first, second)

    def test_different_sources_produce_different_ids(self):
        first = make_batch_id("olist_orders", "batch-001")
        second = make_batch_id("olist_payments", "batch-001")
        self.assertNotEqual(first, second)

    def test_batch_id_has_expected_prefix_and_length(self):
        batch_id = make_batch_id("olist_orders", "batch-001")
        self.assertTrue(batch_id.startswith("batch_"))
        self.assertEqual(len(batch_id), 38)

    def test_empty_source_is_rejected(self):
        with self.assertRaises(ValueError):
            make_batch_id("", "batch-001")

    def test_empty_batch_key_is_rejected(self):
        with self.assertRaises(ValueError):
            make_batch_id("olist_orders", " ")

    def test_whitespace_is_normalized(self):
        first = make_batch_id(" olist_orders ", " batch-001 ")
        second = make_batch_id("olist_orders", "batch-001")
        self.assertEqual(first, second)


if __name__ == "__main__":
    unittest.main()


class TestIncrementalWriterValidation(unittest.TestCase):
    """Validate records before any database write is attempted."""

    def setUp(self):
        from unittest.mock import patch
        self.connect_patcher = patch(
            "sales_pipeline.incremental.psycopg.connect",
            side_effect=AssertionError(
                "Database connection should not be attempted"
            ),
        )
        self.connect_patcher.start()
        self.addCleanup(self.connect_patcher.stop)

    def test_non_list_records_rejected(self):
        from sales_pipeline.incremental import write_incremental_orders

        with self.assertRaises(TypeError):
            write_incremental_orders("unused", "batch_test", None)

    def test_non_dictionary_record_rejected(self):
        from sales_pipeline.incremental import write_incremental_orders

        with self.assertRaisesRegex(TypeError, "must be a dictionary"):
            write_incremental_orders("unused", "batch_test", ["invalid"])

    def test_missing_fields_rejected(self):
        from sales_pipeline.incremental import write_incremental_orders

        with self.assertRaisesRegex(ValueError, "missing fields"):
            write_incremental_orders(
                "unused",
                "batch_test",
                [{"order_id": "order_1"}],
            )

    def test_blank_order_id_rejected(self):
        from sales_pipeline.incremental import write_incremental_orders

        record = {
            "order_id": " ",
            "customer_id": "customer_1",
            "order_status": "processing",
            "order_purchase_timestamp": None,
        }
        with self.assertRaisesRegex(ValueError, "invalid order_id"):
            write_incremental_orders("unused", "batch_test", [record])

    def test_blank_customer_id_rejected(self):
        from sales_pipeline.incremental import write_incremental_orders

        record = {
            "order_id": "order_1",
            "customer_id": " ",
            "order_status": "processing",
            "order_purchase_timestamp": None,
        }
        with self.assertRaisesRegex(ValueError, "invalid customer_id"):
            write_incremental_orders("unused", "batch_test", [record])

    def test_duplicate_order_ids_rejected(self):
        from sales_pipeline.incremental import write_incremental_orders

        record = {
            "order_id": "order_1",
            "customer_id": "customer_1",
            "order_status": "processing",
            "order_purchase_timestamp": None,
        }
        with self.assertRaisesRegex(ValueError, "Duplicate order_id"):
            write_incremental_orders(
                "unused",
                "batch_test",
                [record, record.copy()],
            )

class TestIncrementalOrderFieldValidation(unittest.TestCase):
    """Tests for order status and purchase timestamp validation."""

    def setUp(self):
        from unittest.mock import patch

        self.connect_patcher = patch(
            "sales_pipeline.incremental.psycopg.connect",
            side_effect=AssertionError(
                "Database connection should not be attempted"
            ),
        )
        self.connect_patcher.start()
        self.addCleanup(self.connect_patcher.stop)

    def setUp_record(self):
        return {
            "order_id": "order_validation_test",
            "customer_id": "customer_validation_test",
            "order_status": "processing",
            "order_purchase_timestamp": None,
        }

    def test_none_timestamp_is_valid(self):
        from sales_pipeline.incremental import _validate_order_fields

        record = self.setUp_record()
        self.assertIsNone(_validate_order_fields(record))

    def test_iso_timestamp_string_is_valid(self):
        from sales_pipeline.incremental import _validate_order_fields

        record = self.setUp_record()
        record["order_purchase_timestamp"] = "2018-04-01 00:27:51"
        self.assertIsNone(_validate_order_fields(record))

    def test_datetime_object_is_valid(self):
        from datetime import datetime
        from sales_pipeline.incremental import _validate_order_fields

        record = self.setUp_record()
        record["order_purchase_timestamp"] = datetime(2018, 4, 1, 0, 27, 51)
        self.assertIsNone(_validate_order_fields(record))

    def test_invalid_timestamp_string_is_rejected(self):
        from sales_pipeline.incremental import _validate_order_fields

        record = self.setUp_record()
        record["order_purchase_timestamp"] = "not-a-timestamp"

        error = _validate_order_fields(record)
        self.assertIsNotNone(error)
        self.assertIn("timestamp", error)

    def test_blank_timestamp_string_is_rejected(self):
        from sales_pipeline.incremental import _validate_order_fields

        record = self.setUp_record()
        record["order_purchase_timestamp"] = "   "

        error = _validate_order_fields(record)
        self.assertIsNotNone(error)
        self.assertIn("timestamp", error)

    def test_blank_order_status_is_rejected(self):
        from sales_pipeline.incremental import _validate_order_fields

        record = self.setUp_record()
        record["order_status"] = " "

        error = _validate_order_fields(record)
        self.assertIsNotNone(error)
        self.assertIn("order_status", error)

    def test_non_string_order_status_is_rejected(self):
        from sales_pipeline.incremental import _validate_order_fields

        record = self.setUp_record()
        record["order_status"] = None

        error = _validate_order_fields(record)
        self.assertIsNotNone(error)
        self.assertIn("order_status", error)

    def test_direct_writer_rejects_invalid_timestamp_before_connect(self):
        from sales_pipeline.incremental import write_incremental_orders

        record = self.setUp_record()
        record["order_purchase_timestamp"] = "invalid-date"

        with self.assertRaisesRegex(ValueError, "timestamp"):
            write_incremental_orders("unused", "batch_validation_test", [record])

    def test_direct_writer_rejects_blank_status_before_connect(self):
        from sales_pipeline.incremental import write_incremental_orders

        record = self.setUp_record()
        record["order_status"] = " "

        with self.assertRaisesRegex(ValueError, "order_status"):
            write_incremental_orders("unused", "batch_validation_test", [record])
