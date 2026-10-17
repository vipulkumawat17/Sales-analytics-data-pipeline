"""Integration tests for incremental batch lifecycle.

Requires NEON_DB_URL to be configured in the environment.
Tests use uniquely identified records in pipeline_batch_runs.
"""

import os
import sys
import unittest
import uuid
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from sales_pipeline.incremental import (
    make_batch_id,
    register_batch,
    mark_batch_failed,
    mark_batch_succeeded,
)


class TestIncrementalBatchIntegration(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.database_url = os.environ.get("NEON_DB_URL")
        if not cls.database_url:
            raise unittest.SkipTest(
                "Set NEON_DB_URL to run database integration tests."
            )

    def new_batch_id(self, label):
        unique_key = f"{label}-{uuid.uuid4().hex}"
        return make_batch_id("integration_test", unique_key)

    def test_successful_batch_rerun_is_skipped(self):
        batch_id = self.new_batch_id("success-rerun")

        self.assertEqual(
            register_batch(self.database_url, batch_id),
            "started",
        )

        mark_batch_succeeded(
            self.database_url,
            batch_id,
            source_rows=3,
            inserted_rows=3,
        )

        self.assertEqual(
            register_batch(self.database_url, batch_id),
            "skipped",
        )

    def test_failed_batch_can_be_retried(self):
        batch_id = self.new_batch_id("failure-retry")

        self.assertEqual(
            register_batch(self.database_url, batch_id),
            "started",
        )

        mark_batch_failed(
            self.database_url,
            batch_id,
            "Controlled integration-test failure",
        )

        self.assertEqual(
            register_batch(self.database_url, batch_id),
            "started",
        )

        mark_batch_succeeded(
            self.database_url,
            batch_id,
            source_rows=5,
            inserted_rows=4,
            updated_rows=1,
        )

        import psycopg
        with psycopg.connect(self.database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT status, attempt_number, source_rows,
                           inserted_rows, updated_rows, error_message
                    FROM public.pipeline_batch_runs
                    WHERE batch_id = %s
                    """,
                    (batch_id,),
                )
                result = cur.fetchone()

        self.assertEqual(
            result,
            ("succeeded", 2, 5, 4, 1, None),
        )


if __name__ == "__main__":
    unittest.main()
