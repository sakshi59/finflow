import os
import json
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

SOURCE_CSV      = "data/PS_20174392719_1491204439457_log.csv"
CONTAINER_NAME  = os.getenv("AZURE_CONTAINER_BRONZE", "bronze")
ADLS_PREFIX     = "transactions"
CHUNK_SIZE      = 500_000
LOAD_TIMESTAMP  = datetime.utcnow()
PARTITION_YEAR  = str(LOAD_TIMESTAMP.year)
PARTITION_MONTH = str(LOAD_TIMESTAMP.month).zfill(2)

EXPECTED_COLUMNS = [
    "step", "type", "amount", "nameOrig", "oldbalanceOrg",
    "newbalanceOrig", "nameDest", "oldbalanceDest",
    "newbalanceDest", "isFraud", "isFlaggedFraud"
]

DTYPE_MAP = {
    "step":           "int32",
    "type":           "string",
    "amount":         "float64",
    "nameOrig":       "string",
    "oldbalanceOrg":  "float64",
    "newbalanceOrig": "float64",
    "nameDest":       "string",
    "oldbalanceDest": "float64",
    "newbalanceDest": "float64",
    "isFraud":        "int8",
    "isFlaggedFraud": "int8",
}

def get_adls_client():
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not connection_string:
        raise ValueError("AZURE_STORAGE_CONNECTION_STRING not found in .env")
    return DataLakeServiceClient.from_connection_string(connection_string)

def upload_bytes_to_adls(client, container, remote_path, data):
    fs_client   = client.get_file_system_client(container)
    file_client = fs_client.get_file_client(remote_path)
    file_client.upload_data(data, overwrite=True)
    log.info(f"  Uploaded -> adls://{container}/{remote_path}")

def ingest_bronze(source_csv):
    adls  = get_adls_client()
    tmp   = Path("tmp_parquet")
    tmp.mkdir(exist_ok=True)

    total_rows     = 0
    chunk_index    = 0
    uploaded_files = []

    partition_path = f"{ADLS_PREFIX}/year={PARTITION_YEAR}/month={PARTITION_MONTH}"

    log.info("=" * 60)
    log.info("FinFlow - Bronze Ingestion Starting")
    log.info(f"  Source : {source_csv}")
    log.info(f"  Target : adls://{CONTAINER_NAME}/{partition_path}/")
    log.info(f"  Chunks : {CHUNK_SIZE:,} rows each")
    log.info("=" * 60)

    reader = pd.read_csv(
        source_csv,
        dtype=DTYPE_MAP,
        chunksize=CHUNK_SIZE,
        usecols=EXPECTED_COLUMNS,
    )

    for chunk_df in reader:
        chunk_index  += 1
        rows_in_chunk = len(chunk_df)
        total_rows   += rows_in_chunk

        log.info(f"  Chunk {chunk_index:02d}: {rows_in_chunk:,} rows | Total so far: {total_rows:,}")

        chunk_df["_ingested_at"] = LOAD_TIMESTAMP.isoformat()
        chunk_df["_source_file"] = Path(source_csv).name
        chunk_df["_chunk_index"] = chunk_index

        arrow_table   = pa.Table.from_pandas(chunk_df, preserve_index=False)
        local_file    = tmp / f"part-{chunk_index:04d}.parquet"
        pq.write_table(arrow_table, local_file, compression="snappy")

        parquet_bytes = local_file.read_bytes()
        remote_path   = f"{partition_path}/part-{chunk_index:04d}.parquet"
        upload_bytes_to_adls(adls, CONTAINER_NAME, remote_path, parquet_bytes)
        uploaded_files.append(remote_path)
        local_file.unlink()

    metadata = {
        "load_timestamp":   LOAD_TIMESTAMP.isoformat(),
        "source_file":      Path(source_csv).name,
        "adls_destination": f"adls://{CONTAINER_NAME}/{partition_path}/",
        "total_rows":       total_rows,
        "total_chunks":     chunk_index,
        "files_uploaded":   uploaded_files,
        "schema":           {col: str(dt) for col, dt in DTYPE_MAP.items()},
        "partition":        {"year": PARTITION_YEAR, "month": PARTITION_MONTH},
        "status":           "SUCCESS",
    }

    metadata_bytes = json.dumps(metadata, indent=2).encode("utf-8")
    upload_bytes_to_adls(
        adls, CONTAINER_NAME,
        f"{partition_path}/_metadata.json",
        metadata_bytes,
    )

    tmp.rmdir()

    log.info("=" * 60)
    log.info("Bronze Ingestion COMPLETE")
    log.info(f"  Total rows   : {total_rows:,}")
    log.info(f"  Parquet files: {chunk_index}")
    log.info(f"  Location     : adls://{CONTAINER_NAME}/{partition_path}/")
    log.info("=" * 60)

    return metadata

if __name__ == "__main__":
    result = ingest_bronze(SOURCE_CSV)
    print(json.dumps(result, indent=2))
