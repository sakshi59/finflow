"""
test_ingest_bronze.py
---------------------
PURPOSE: Verify the Bronze ingestion worked correctly.
         Tests run automatically in CI/CD on every GitHub push.

WHY TESTS: In real companies, broken pipelines get caught in CI
before they ever touch production data. Without tests, you find
out a pipeline failed when someone's dashboard goes blank at 9am.
"""

import json
import boto3
import os
from dotenv import load_dotenv

load_dotenv()

BUCKET = "finflow-data-lake"
PREFIX = "bronze/transactions"

def get_s3():
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )


def test_parquet_files_exist():
    """At least one Parquet file must exist under bronze/transactions/"""
    s3 = get_s3()
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX)
    parquet_files = [
        obj for obj in response.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ]
    assert len(parquet_files) > 0, (
        f"No Parquet files found under s3://{BUCKET}/{PREFIX}/"
    )
    print(f"  PASS: {len(parquet_files)} Parquet file(s) found")


def test_metadata_file_exists():
    """A _metadata.json file must exist — proves the load completed fully."""
    s3 = get_s3()
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX)
    metadata_files = [
        obj for obj in response.get("Contents", [])
        if obj["Key"].endswith("_metadata.json")
    ]
    assert len(metadata_files) > 0, "No _metadata.json found — load may be incomplete"
    print(f"  PASS: metadata file found")


def test_metadata_row_count_is_reasonable():
    """
    Row count in metadata must be > 5 million.
    PaySim has 6.3M rows — if this fails, something truncated the data.
    WHY: Silent data loss is the most dangerous pipeline failure.
         The pipeline 'succeeds' but you loaded 10% of the data.
    """
    s3 = get_s3()
    # find the metadata file
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX)
    metadata_key = next(
        obj["Key"] for obj in response.get("Contents", [])
        if obj["Key"].endswith("_metadata.json")
    )
    obj = s3.get_object(Bucket=BUCKET, Key=metadata_key)
    metadata = json.loads(obj["Body"].read())

    total_rows = metadata["total_rows"]
    assert total_rows > 5_000_000, (
        f"Expected > 5M rows, got {total_rows:,} — possible data loss"
    )
    print(f"  PASS: {total_rows:,} rows loaded")


def test_metadata_schema_contains_expected_columns():
    """Schema in metadata must contain all expected columns."""
    s3 = get_s3()
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX)
    metadata_key = next(
        obj["Key"] for obj in response.get("Contents", [])
        if obj["Key"].endswith("_metadata.json")
    )
    obj = s3.get_object(Bucket=BUCKET, Key=metadata_key)
    metadata = json.loads(obj["Body"].read())

    expected = {"step", "type", "amount", "nameOrig", "isFraud"}
    actual   = set(metadata["schema"].keys())
    missing  = expected - actual
    assert not missing, f"Missing columns in schema: {missing}"
    print(f"  PASS: all expected columns present in schema")


def test_metadata_status_is_success():
    """Status field must be SUCCESS — not PARTIAL or FAILED."""
    s3 = get_s3()
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX)
    metadata_key = next(
        obj["Key"] for obj in response.get("Contents", [])
        if obj["Key"].endswith("_metadata.json")
    )
    obj = s3.get_object(Bucket=BUCKET, Key=metadata_key)
    metadata = json.loads(obj["Body"].read())
    assert metadata["status"] == "SUCCESS", (
        f"Load status is {metadata['status']}, expected SUCCESS"
    )
    print(f"  PASS: status is SUCCESS")


if __name__ == "__main__":
    print("Running Bronze ingestion tests...\n")
    tests = [
        test_parquet_files_exist,
        test_metadata_file_exists,
        test_metadata_row_count_is_reasonable,
        test_metadata_schema_contains_expected_columns,
        test_metadata_status_is_success,
    ]
    passed = 0
    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"  FAIL: {test.__name__} — {e}")

    print(f"\n{passed}/{len(tests)} tests passed")
