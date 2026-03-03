"""
Transform Silver Layer (ETL step)

- Extract: staging_employee / staging_timesheet
- Transform: clean, deduplicate, convert types
- Load: employee / timesheet tables in ETL DB
"""

# src/etl/transform_silver.py
"""
Transform Silver Layer (ETL step)

- staging_employee -> employee
- staging_timesheet -> timesheet
- Preserves schema and uses chunks for large datasets
"""

import logging
from etl.db import get_engine
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

engine = get_engine()
CHUNKSIZE = 5000


# =========================
# EMPLOYEE TRANSFORM
# =========================
def transform_employee():
    try:
        df_iter = pd.read_sql(
            "SELECT * FROM staging_employee",
            engine,
            chunksize=CHUNKSIZE
        )

        # Truncate but preserve schema
        with engine.begin() as conn:
            conn.execute("TRUNCATE TABLE employee RESTART IDENTITY CASCADE")

        total_rows = 0

        for chunk in df_iter:

            # SAFE TRIM (no astype, no crash)
            for col in chunk.columns:
                if chunk[col].dtype == "object":
                    chunk[col] = chunk[col].apply(
                        lambda x: x.strip() if isinstance(x, str) else x
                    )

            chunk.drop_duplicates(subset="client_employee_id", inplace=True)
            chunk.dropna(axis=1, how="all", inplace=True)
            chunk.dropna(axis=0, how="all", inplace=True)

            chunk = chunk[chunk["client_employee_id"].notna()]

            # Dates
            date_cols = [
                "dob", "hire_date", "recent_hire_date",
                "anniversary_date", "term_date", "job_start_date"
            ]
            for col in date_cols:
                if col in chunk.columns:
                    chunk[col] = pd.to_datetime(chunk[col], errors="coerce")

            # Numeric
            num_cols = [
                "years_of_experience",
                "scheduled_weekly_hour",
                "active_status",
                "job_code"
            ]
            for col in num_cols:
                if col in chunk.columns:
                    chunk[col] = pd.to_numeric(chunk[col], errors="coerce")

            chunk.to_sql("employee", engine, if_exists="append", index=False)

            total_rows += len(chunk)
            logging.info(f"Inserted {len(chunk)} employee rows")

        logging.info(f"Total employee rows loaded: {total_rows}")

    except Exception as e:
        logging.error(f"Employee transform failed: {e}")
        raise


# =========================
# TIMESHEET TRANSFORM
# =========================
def transform_timesheet():
    try:
        df_iter = pd.read_sql(
            "SELECT * FROM staging_timesheet",
            engine,
            chunksize=CHUNKSIZE
        )

        # Load valid employee IDs
        employee_df = pd.read_sql(
            "SELECT client_employee_id FROM employee",
            engine
        )

        valid_employee_ids = set(
            employee_df["client_employee_id"].astype(str)
        )

        with engine.begin() as conn:
            conn.execute("TRUNCATE TABLE timesheet RESTART IDENTITY CASCADE")

        total_inserted = 0
        total_skipped = 0

        for chunk in df_iter:

            # SAFE TRIM
            for col in chunk.columns:
                if chunk[col].dtype == "object":
                    chunk[col] = chunk[col].apply(
                        lambda x: x.strip() if isinstance(x, str) else x
                    )

            chunk.drop_duplicates(inplace=True)
            chunk.dropna(axis=1, how="all", inplace=True)
            chunk.dropna(axis=0, how="all", inplace=True)

            chunk = chunk[chunk["client_employee_id"].notna()]

            # Dates
            date_cols = [
                "punch_apply_date",
                "punch_in_datetime",
                "punch_out_datetime",
                "scheduled_start_datetime",
                "scheduled_end_datetime"
            ]
            for col in date_cols:
                if col in chunk.columns:
                    chunk[col] = pd.to_datetime(chunk[col], errors="coerce")

            # Numeric
            if "hours_worked" in chunk.columns:
                chunk["hours_worked"] = pd.to_numeric(
                    chunk["hours_worked"],
                    errors="coerce"
                )

            # FK FILTER
            before = len(chunk)

            chunk = chunk[
                chunk["client_employee_id"].astype(str)
                .isin(valid_employee_ids)
            ]

            skipped = before - len(chunk)
            total_skipped += skipped

            if chunk.empty:
                continue

            chunk.to_sql(
                "timesheet",
                engine,
                if_exists="append",
                index=False,
                method="multi"
            )

            total_inserted += len(chunk)
            logging.info(f"Inserted {len(chunk)} timesheet rows")

        logging.info(f"Total inserted: {total_inserted}")
        logging.warning(f"Total skipped (missing employee): {total_skipped}")

    except Exception as e:
        logging.error(f"Timesheet transform failed: {e}")
        raise
