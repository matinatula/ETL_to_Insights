# src/etl/extract_bronze.py
"""
Extract raw CSVs -> staging tables (Bronze layer)
- CSV -> staging_employee / staging_timesheet
- Preserves table types (use append after TRUNCATE)
"""

# src/etl/extract_bronze.py
"""
Extract raw CSVs -> staging tables (Bronze layer)
Supports:
- Local files
- MinIO (S3-compatible)
- Large files via chunksize
"""

from dotenv import load_dotenv
from airflow.utils.log.logging_mixin import LoggingMixin
from etl.db import get_engine
from io import BytesIO
import boto3
import pandas as pd
import glob
import os

load_dotenv()
log = LoggingMixin().log

# Config

SOURCE_TYPE = os.getenv("SOURCE_TYPE", "local")

EMPLOYEE_CSV = os.getenv("EMPLOYEE_CSV")
TIMESHEETS_FOLDER = os.getenv("TIMESHEETS_FOLDER")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
MINIO_EMPLOYEE_OBJECT = os.getenv("MINIO_EMPLOYEE_OBJECT")
MINIO_TIMESHEETS_PREFIX = os.getenv("MINIO_TIMESHEETS_PREFIX")

engine = get_engine()
CHUNKSIZE = 5000


# MinIO Client

def get_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


# Employee Extraction

def extract_employee():
    log.info(f"Starting employee extraction (SOURCE_TYPE={SOURCE_TYPE})")

    try:
        with engine.begin() as conn:
            conn.execute("TRUNCATE TABLE staging_employee RESTART IDENTITY")

        total_rows = 0

        if SOURCE_TYPE == "local":
            if not os.path.exists(EMPLOYEE_CSV):
                log.error(f"Employee file not found: {EMPLOYEE_CSV}")
                return

            for chunk in pd.read_csv(EMPLOYEE_CSV, chunksize=CHUNKSIZE, sep="|"):
                chunk.to_sql("staging_employee", engine,
                             if_exists="append", index=False)
                total_rows += len(chunk)

        elif SOURCE_TYPE == "minio":
            s3 = get_minio_client()
            response = s3.get_object(
                Bucket=MINIO_BUCKET,
                Key=MINIO_EMPLOYEE_OBJECT
            )
            data = response["Body"].read()

            for chunk in pd.read_csv(BytesIO(data), chunksize=CHUNKSIZE, sep="|"):
                chunk.to_sql("staging_employee", engine,
                             if_exists="append", index=False)
                total_rows += len(chunk)

        else:
            raise ValueError("Invalid SOURCE_TYPE")

        log.info(f"Total employee rows loaded: {total_rows}")

    except Exception as e:
        log.exception(f"Failed to extract employee: {e}")


# Timesheet Extraction

def extract_timesheets():
    log.info(f"Starting timesheet extraction (SOURCE_TYPE={SOURCE_TYPE})")

    try:
        with engine.begin() as conn:
            conn.execute("TRUNCATE TABLE staging_timesheet RESTART IDENTITY")

        total_rows = 0

        if SOURCE_TYPE == "local":
            if not os.path.exists(TIMESHEETS_FOLDER):
                log.error(f"Timesheet folder not found: {TIMESHEETS_FOLDER}")
                return

            all_files = glob.glob(os.path.join(TIMESHEETS_FOLDER, "*.csv"))

            for file in all_files:
                for chunk in pd.read_csv(file, chunksize=CHUNKSIZE, sep="|"):
                    chunk.to_sql("staging_timesheet", engine,
                                 if_exists="append", index=False)
                    total_rows += len(chunk)

        elif SOURCE_TYPE == "minio":
            s3 = get_minio_client()

            objects = s3.list_objects_v2(
                Bucket=MINIO_BUCKET,
                Prefix=MINIO_TIMESHEETS_PREFIX
            )

            for obj in objects.get("Contents", []):
                key = obj["Key"]

                if key.endswith(".csv"):
                    response = s3.get_object(Bucket=MINIO_BUCKET, Key=key)
                    data = response["Body"].read()

                    for chunk in pd.read_csv(BytesIO(data), chunksize=CHUNKSIZE, sep="|"):
                        chunk.to_sql("staging_timesheet", engine,
                                     if_exists="append", index=False)
                        total_rows += len(chunk)

        else:
            raise ValueError("Invalid SOURCE_TYPE")

        log.info(f"Total timesheet rows loaded: {total_rows}")

    except Exception as e:
        log.exception(f"Failed to extract timesheets: {e}")
