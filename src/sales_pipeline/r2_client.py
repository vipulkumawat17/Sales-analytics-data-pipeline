
"""Cloudflare R2 object-storage utilities.

Supports Colab Secrets and explicitly supplied credentials.
Uses boto3's S3-compatible API; does not require Hadoop S3A.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Iterator

import boto3
from botocore.exceptions import ClientError


def create_r2_client(
    account_id: str,
    access_key_id: str,
    secret_access_key: str,
):
    """Create a boto3 client for Cloudflare R2."""
    values = [account_id, access_key_id, secret_access_key]
    if not all(value and value.strip() for value in values):
        raise ValueError("R2 credentials must not be empty.")

    return boto3.client(
        "s3",
        endpoint_url=f"https://{account_id.strip()}.r2.cloudflarestorage.com",
        aws_access_key_id=access_key_id.strip(),
        aws_secret_access_key=secret_access_key.strip(),
        region_name="auto",
    )


def create_r2_client_from_colab():
    """Load R2 credentials using the existing Colab Secret names."""
    try:
        from google.colab import userdata
    except ImportError as exc:
        raise RuntimeError(
            "Colab Secrets are unavailable. Use create_r2_client() "
            "with explicitly supplied credentials."
        ) from exc

    account_id = userdata.get("R2_ACCOUNT_ID").strip()
    access_key_id = userdata.get("R2_ACCESS_KEY").strip()
    secret_access_key = userdata.get("R2_SECRET_ACCESS_KEY").strip()

    return create_r2_client(
        account_id,
        access_key_id,
        secret_access_key,
    )


def get_bucket_name_from_colab():
    """Load the R2 bucket name using the existing Colab Secret name."""
    from google.colab import userdata

    return userdata.get("R2_BUCKET-NAME").strip()


def verify_bucket(client, bucket_name: str) -> bool:
    """Verify access to an R2 bucket."""
    if not bucket_name or not bucket_name.strip():
        raise ValueError("Bucket name must not be empty.")

    client.head_bucket(Bucket=bucket_name.strip())
    return True


def list_objects(
    client,
    bucket_name: str,
    prefix: str = "",
) -> Iterator[dict[str, Any]]:
    """Yield object metadata under a bucket prefix."""
    paginator = client.get_paginator("list_objects_v2")

    for page in paginator.paginate(
        Bucket=bucket_name,
        Prefix=prefix,
    ):
        yield from page.get("Contents", [])


def calculate_sha256(file_path: str | Path) -> str:
    """Calculate a local file's SHA-256 checksum."""
    digest = hashlib.sha256()

    with Path(file_path).open("rb") as file:
        for chunk in iter(lambda: file.read(8 * 1024 * 1024), b""):
            digest.update(chunk)

    return digest.hexdigest()


def download_file(
    client,
    bucket_name: str,
    object_key: str,
    destination: str | Path,
) -> Path:
    """Download an R2 object to a local file."""
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)

    client.download_file(
        bucket_name,
        object_key,
        str(destination),
    )
    return destination


def upload_parquet(
    client,
    bucket_name: str,
    local_path: str | Path,
    object_key: str,
    overwrite: bool = False,
) -> dict[str, Any]:
    """Upload a Parquet file and attach its SHA-256 metadata."""
    local_path = Path(local_path)

    if not local_path.is_file():
        raise FileNotFoundError(local_path)

    if local_path.suffix.lower() != ".parquet":
        raise ValueError("Only .parquet files can be uploaded.")

    if not object_key or not object_key.strip():
        raise ValueError("Object key must not be empty.")

    if not overwrite:
        try:
            client.head_object(
                Bucket=bucket_name,
                Key=object_key,
            )
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            status = exc.response.get(
                "ResponseMetadata", {}
            ).get("HTTPStatusCode")

            if code not in ("404", "NoSuchKey", "NotFound") and status != 404:
                raise
        else:
            raise FileExistsError(
                f"R2 object already exists: {object_key}"
            )

    checksum = calculate_sha256(local_path)

    client.upload_file(
        str(local_path),
        bucket_name,
        object_key,
        ExtraArgs={
            "Metadata": {"sha256": checksum},
            "ContentType": "application/vnd.apache.parquet",
        },
    )

    return {
        "object_key": object_key,
        "size_bytes": local_path.stat().st_size,
        "sha256": checksum,
    }


def verify_object(
    client,
    bucket_name: str,
    object_key: str,
    local_path: str | Path | None = None,
) -> dict[str, Any]:
    """Verify remote size and SHA-256 metadata.

    If local_path is provided, also compare the local checksum.
    """
    response = client.head_object(
        Bucket=bucket_name,
        Key=object_key,
    )

    metadata = response.get("Metadata", {})
    result = {
        "object_key": object_key,
        "remote_size_bytes": response["ContentLength"],
        "remote_sha256_metadata": metadata.get("sha256"),
        "exists": True,
    }

    if local_path is not None:
        local_path = Path(local_path)
        local_checksum = calculate_sha256(local_path)

        result["local_size_bytes"] = local_path.stat().st_size
        result["local_sha256"] = local_checksum
        result["size_matches"] = (
            local_path.stat().st_size == response["ContentLength"]
        )
        result["checksum_matches"] = (
            local_checksum == metadata.get("sha256")
        )

    return result


def read_parquet_from_r2(
    spark,
    client,
    bucket_name: str,
    object_key: str,
    local_dir: str | Path,
):
    """Download one R2 Parquet object and return it as a Spark DataFrame."""
    local_dir = Path(local_dir)
    local_dir.mkdir(parents=True, exist_ok=True)

    local_path = local_dir / Path(object_key).name

    download_file(
        client=client,
        bucket_name=bucket_name,
        object_key=object_key,
        destination=local_path,
    )

    dataframe = spark.read.parquet(str(local_path))
    return dataframe



def write_spark_parquet_to_r2(
    dataframe,
    client,
    bucket_name: str,
    prefix: str,
    local_dir: str | Path,
    mode: str = "error",
):
    """
    Write Spark output as Parquet and upload all part files to R2.

    mode='error':
        Fail if the destination prefix already contains objects.

    mode='overwrite':
        Upload and verify the new output first, then remove stale
        objects from the destination prefix.

    Failed uploads trigger cleanup of objects created by this run.
    """
    import tempfile

    if mode not in ("error", "overwrite"):
        raise ValueError("mode must be 'error' or 'overwrite'.")

    prefix = prefix.strip("/")

    if not prefix:
        raise ValueError(
            "A non-empty destination prefix is required."
        )

    local_dir = Path(local_dir)
    local_dir.mkdir(parents=True, exist_ok=True)

    # Inspect existing destination objects before processing.
    existing_objects = list(
        list_objects(client, bucket_name, prefix=prefix + "/")
    )

    if mode == "error" and existing_objects:
        raise FileExistsError(
            f"Destination prefix already contains objects: {prefix}"
        )

    uploaded_objects = []
    uploaded_keys = []

    try:
        # Use an isolated temporary directory. Do not erase local_dir.
        with tempfile.TemporaryDirectory(
            prefix="spark-r2-",
            dir=str(local_dir),
        ) as temporary_directory:

            output_path = Path(temporary_directory) / "parquet_output"

            dataframe.write.mode("error").parquet(
                str(output_path)
            )

            parquet_files = sorted(
                output_path.glob("*.parquet")
            )

            if not parquet_files:
                raise RuntimeError(
                    "Spark did not produce any Parquet part files."
                )

            # Upload and verify every new part before deleting old data.
            for parquet_file in parquet_files:
                object_key = (
                    f"{prefix}/{parquet_file.name}"
                )

                result = upload_parquet(
                    client=client,
                    bucket_name=bucket_name,
                    local_path=parquet_file,
                    object_key=object_key,
                    overwrite=False,
                )

                verification = verify_object(
                    client=client,
                    bucket_name=bucket_name,
                    object_key=object_key,
                    local_path=parquet_file,
                )

                if not (
                    verification["size_matches"]
                    and verification["checksum_matches"]
                ):
                    raise RuntimeError(
                        f"Upload verification failed: {object_key}"
                    )

                uploaded_objects.append(result)
                uploaded_keys.append(object_key)

        # Remove stale objects only after all new uploads are verified.
        if mode == "overwrite":
            stale_keys = [
                item["Key"]
                for item in existing_objects
                if item["Key"] not in uploaded_keys
            ]

            # S3-compatible DeleteObjects supports batches of 1000.
            for offset in range(0, len(stale_keys), 1000):
                batch = stale_keys[offset:offset + 1000]

                response = client.delete_objects(
                    Bucket=bucket_name,
                    Delete={
                        "Objects": [
                            {"Key": key} for key in batch
                        ],
                        "Quiet": False,
                    },
                )

                errors = response.get("Errors", [])

                if errors:
                    raise RuntimeError(
                        f"Failed to delete stale R2 objects: {errors}"
                    )

    except Exception:
        # Roll back only objects uploaded by this execution.
        if uploaded_keys:
            for offset in range(0, len(uploaded_keys), 1000):
                batch = uploaded_keys[offset:offset + 1000]

                client.delete_objects(
                    Bucket=bucket_name,
                    Delete={
                        "Objects": [
                            {"Key": key} for key in batch
                        ],
                        "Quiet": True,
                    },
                )

        raise

    return {
        "uploaded_file_count": len(uploaded_objects),
        "uploaded_objects": uploaded_objects,
        "total_size_bytes": sum(
            item["size_bytes"] for item in uploaded_objects
        ),
        "destination_prefix": prefix,
        "mode": mode,
        "verification": "PASS",
    }




def read_parquet_prefix_from_r2(
    spark,
    client,
    bucket_name: str,
    prefix: str,
    local_dir: str | Path,
):
    """
    Download and verify all Parquet objects under an R2 prefix,
    then return them as one Spark DataFrame.

    Local files are retained so Spark can evaluate its lazy query plan.
    Only objects discovered under the requested R2 prefix are read.
    """
    prefix = prefix.strip("/")

    if not prefix:
        raise ValueError("A non-empty R2 prefix is required.")

    objects = [
        obj for obj in list_objects(
            client,
            bucket_name,
            prefix=prefix + "/",
        )
        if obj.get("Key", "").lower().endswith(".parquet")
    ]

    if not objects:
        raise FileNotFoundError(
            f"No Parquet objects found under R2 prefix: {prefix}"
        )

    staging_dir = (
        Path(local_dir)
        / "r2_cache"
        / prefix.replace("/", "__")
    )
    staging_dir.mkdir(parents=True, exist_ok=True)

    local_files = []

    for obj in objects:
        object_key = obj["Key"]
        relative_key = object_key[len(prefix):].lstrip("/")
        destination = staging_dir / relative_key

        download_file(
            client=client,
            bucket_name=bucket_name,
            object_key=object_key,
            destination=destination,
        )

        verification = verify_object(
            client=client,
            bucket_name=bucket_name,
            object_key=object_key,
            local_path=destination,
        )

        if not (
            verification["size_matches"]
            and verification["checksum_matches"]
        ):
            raise RuntimeError(
                f"R2 download verification failed: {object_key}"
            )

        local_files.append(str(destination))

    return spark.read.parquet(*local_files)
