"""Unit tests for Cloudflare R2 storage integration."""

import sys
import tempfile
import unittest
import hashlib

from pathlib import Path
from unittest.mock import Mock, patch

from botocore.exceptions import ClientError

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from sales_pipeline import r2_client


class TestR2Client(unittest.TestCase):

    def test_create_r2_client(self):
        with patch.object(r2_client.boto3, "client") as mock_client:
            r2_client.create_r2_client(
                "account123",
                "access123",
                "secret123"
            )

            mock_client.assert_called_once()

    def test_empty_credentials_raise_error(self):
        with self.assertRaises(ValueError):
            r2_client.create_r2_client("", "access", "secret")

    def test_calculate_sha256(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "sample.txt"
            path.write_bytes(b"sales pipeline")

            expected = hashlib.sha256(
                b"sales pipeline"
            ).hexdigest()

            self.assertEqual(
                r2_client.calculate_sha256(path),
                expected
            )

    def test_upload_parquet_success(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "sample.parquet"
            path.write_bytes(b"parquet test data")

            client = Mock()
            client.head_object.side_effect = ClientError(
                {
                    "Error": {"Code": "404"},
                    "ResponseMetadata": {"HTTPStatusCode": 404}
                },
                "HeadObject"
            )

            result = r2_client.upload_parquet(
                client,
                "test-bucket",
                path,
                "test/sample.parquet"
            )

            self.assertEqual(
                result["size_bytes"],
                path.stat().st_size
            )
            self.assertEqual(
                result["sha256"],
                r2_client.calculate_sha256(path)
            )
            client.upload_file.assert_called_once()

    def test_upload_rejects_existing_object(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "sample.parquet"
            path.write_bytes(b"test")

            client = Mock()
            client.head_object.return_value = {
                "ContentLength": 4
            }

            with self.assertRaises(FileExistsError):
                r2_client.upload_parquet(
                    client,
                    "test-bucket",
                    path,
                    "existing/sample.parquet",
                    overwrite=False
                )

            client.upload_file.assert_not_called()

    def test_upload_rejects_non_parquet_file(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "sample.csv"
            path.write_text("a,b")

            with self.assertRaises(ValueError):
                r2_client.upload_parquet(
                    Mock(),
                    "test-bucket",
                    path,
                    "sample.csv"
                )

    def test_download_file(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            destination = Path(temp_dir) / "download.parquet"

            def mock_download(bucket, key, filename):
                Path(filename).write_bytes(b"downloaded data")

            client = Mock()
            client.download_file.side_effect = mock_download

            result = r2_client.download_file(
                client,
                "test-bucket",
                "source/sample.parquet",
                destination
            )

            self.assertEqual(result, destination)
            self.assertTrue(destination.exists())
            self.assertEqual(
                destination.read_bytes(),
                b"downloaded data"
            )

    def test_verify_object_checksum(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "sample.parquet"
            path.write_bytes(b"verified data")

            checksum = r2_client.calculate_sha256(path)

            client = Mock()
            client.head_object.return_value = {
                "ContentLength": path.stat().st_size,
                "Metadata": {"sha256": checksum}
            }

            result = r2_client.verify_object(
                client,
                "test-bucket",
                "sample.parquet",
                path
            )

            self.assertTrue(result["exists"])
            self.assertTrue(result["size_matches"])
            self.assertTrue(result["checksum_matches"])


if __name__ == "__main__":
    unittest.main()


class TestR2SparkHelpers(unittest.TestCase):

    def test_read_parquet_from_r2(self):
        from unittest.mock import MagicMock, patch
        from sales_pipeline.r2_client import read_parquet_from_r2

        spark = MagicMock()
        spark.read.parquet.return_value = "mock_dataframe"

        with patch(
            "sales_pipeline.r2_client.download_file"
        ) as mock_download:
            result = read_parquet_from_r2(
                spark=spark,
                client=MagicMock(),
                bucket_name="test-bucket",
                object_key="bronze/orders/part-000.parquet",
                local_dir=Path(self._testMethodName)
            )

        self.assertEqual(result, "mock_dataframe")
        mock_download.assert_called_once()
        spark.read.parquet.assert_called_once()

    def test_write_spark_parquet_to_r2_uploads_parts(self):
        from unittest.mock import MagicMock, patch
        from sales_pipeline.r2_client import write_spark_parquet_to_r2

        dataframe = MagicMock()
        writer = dataframe.write
        writer.mode.return_value = writer

        def fake_parquet(path):
            output = Path(path)
            output.mkdir(parents=True, exist_ok=True)
            (output / "part-00000.parquet").write_bytes(b"part-one")
            (output / "part-00001.parquet").write_bytes(b"part-two")

        writer.parquet.side_effect = fake_parquet

        client = MagicMock()

        with patch(
            "sales_pipeline.r2_client.list_objects",
            return_value=[]
        ), patch(
            "sales_pipeline.r2_client.upload_parquet",
            side_effect=lambda client, bucket_name, local_path, object_key,
            overwrite=False: {
                "object_key": object_key,
                "size_bytes": Path(local_path).stat().st_size,
                "sha256": "mock-checksum"
            }
        ), patch(
            "sales_pipeline.r2_client.verify_object",
            return_value={
                "exists": True,
                "size_matches": True,
                "checksum_matches": True,
                "verified": True
            }
        ):
            import tempfile

            with tempfile.TemporaryDirectory() as temp_dir:
                result = write_spark_parquet_to_r2(
                    dataframe=dataframe,
                    client=client,
                    bucket_name="test-bucket",
                    prefix="phase7-test/unit-test/",
                    local_dir=Path(temp_dir),
                    mode="error"
                )

        self.assertEqual(result["uploaded_file_count"], 2)
        self.assertEqual(result["total_size_bytes"], len(b"part-one") + len(b"part-two"))
        self.assertTrue(result["verification"] == "PASS")

    def test_write_rejects_existing_prefix_in_error_mode(self):
        from unittest.mock import MagicMock, patch
        from sales_pipeline.r2_client import write_spark_parquet_to_r2

        with patch(
            "sales_pipeline.r2_client.list_objects",
            return_value=[{"Key": "phase7-test/existing/file.parquet"}]
        ):
            with self.assertRaises(Exception):
                write_spark_parquet_to_r2(
                    dataframe=MagicMock(),
                    client=MagicMock(),
                    bucket_name="test-bucket",
                    prefix="phase7-test/existing/",
                    local_dir=Path(self._testMethodName),
                    mode="error"
                )

    def test_write_rejects_empty_prefix(self):
        from unittest.mock import MagicMock
        from sales_pipeline.r2_client import write_spark_parquet_to_r2

        with self.assertRaises(ValueError):
            write_spark_parquet_to_r2(
                dataframe=MagicMock(),
                client=MagicMock(),
                bucket_name="test-bucket",
                prefix="",
                local_dir=Path(self._testMethodName),
                mode="error"
            )

    def test_write_rejects_invalid_mode(self):
        from unittest.mock import MagicMock
        from sales_pipeline.r2_client import write_spark_parquet_to_r2

        with self.assertRaises(ValueError):
            write_spark_parquet_to_r2(
                dataframe=MagicMock(),
                client=MagicMock(),
                bucket_name="test-bucket",
                prefix="phase7-test/invalid-mode/",
                local_dir=Path(self._testMethodName),
                mode="invalid"
            )


class TestR2SparkHelpers(unittest.TestCase):

    def test_read_parquet_from_r2(self):
        from unittest.mock import MagicMock, patch
        from sales_pipeline.r2_client import read_parquet_from_r2

        spark = MagicMock()
        spark.read.parquet.return_value = "mock_dataframe"

        with patch(
            "sales_pipeline.r2_client.download_file"
        ) as mock_download:
            result = read_parquet_from_r2(
                spark=spark,
                client=MagicMock(),
                bucket_name="test-bucket",
                object_key="bronze/orders/part-000.parquet",
                local_dir=Path(self._testMethodName)
            )

        self.assertEqual(result, "mock_dataframe")
        mock_download.assert_called_once()
        spark.read.parquet.assert_called_once()

    def test_write_spark_parquet_to_r2_uploads_parts(self):
        from unittest.mock import MagicMock, patch
        from sales_pipeline.r2_client import write_spark_parquet_to_r2

        dataframe = MagicMock()
        writer = dataframe.write
        writer.mode.return_value = writer

        def fake_parquet(path):
            output = Path(path)
            output.mkdir(parents=True, exist_ok=True)
            (output / "part-00000.parquet").write_bytes(b"part-one")
            (output / "part-00001.parquet").write_bytes(b"part-two")

        writer.parquet.side_effect = fake_parquet

        client = MagicMock()

        with patch(
            "sales_pipeline.r2_client.list_objects",
            return_value=[]
        ), patch(
            "sales_pipeline.r2_client.upload_parquet",
            side_effect=lambda client, bucket_name, local_path, object_key,
            overwrite=False: {
                "object_key": object_key,
                "size_bytes": Path(local_path).stat().st_size,
                "sha256": "mock-checksum"
            }
        ), patch(
            "sales_pipeline.r2_client.verify_object",
            return_value={
                "exists": True,
                "size_matches": True,
                "checksum_matches": True,
                "verified": True
            }
        ):
            import tempfile

            with tempfile.TemporaryDirectory() as temp_dir:
                result = write_spark_parquet_to_r2(
                    dataframe=dataframe,
                    client=client,
                    bucket_name="test-bucket",
                    prefix="phase7-test/unit-test/",
                    local_dir=Path(temp_dir),
                    mode="error"
                )

        self.assertEqual(result["uploaded_file_count"], 2)
        self.assertEqual(result["total_size_bytes"], len(b"part-one") + len(b"part-two"))
        self.assertTrue(result["verification"] == "PASS")

    def test_write_rejects_existing_prefix_in_error_mode(self):
        from unittest.mock import MagicMock, patch
        from sales_pipeline.r2_client import write_spark_parquet_to_r2

        with patch(
            "sales_pipeline.r2_client.list_objects",
            return_value=[{"Key": "phase7-test/existing/file.parquet"}]
        ):
            with self.assertRaises(Exception):
                write_spark_parquet_to_r2(
                    dataframe=MagicMock(),
                    client=MagicMock(),
                    bucket_name="test-bucket",
                    prefix="phase7-test/existing/",
                    local_dir=Path(self._testMethodName),
                    mode="error"
                )

    def test_write_rejects_empty_prefix(self):
        from unittest.mock import MagicMock
        from sales_pipeline.r2_client import write_spark_parquet_to_r2

        with self.assertRaises(ValueError):
            write_spark_parquet_to_r2(
                dataframe=MagicMock(),
                client=MagicMock(),
                bucket_name="test-bucket",
                prefix="",
                local_dir=Path(self._testMethodName),
                mode="error"
            )

    def test_write_rejects_invalid_mode(self):
        from unittest.mock import MagicMock
        from sales_pipeline.r2_client import write_spark_parquet_to_r2

        with self.assertRaises(ValueError):
            write_spark_parquet_to_r2(
                dataframe=MagicMock(),
                client=MagicMock(),
                bucket_name="test-bucket",
                prefix="phase7-test/invalid-mode/",
                local_dir=Path(self._testMethodName),
                mode="invalid"
            )
