from etl.derived import persist_derived_timesheet
from etl.load import load_employee, load_timesheet
from etl.transform import transform_employee, transform_timesheet
from etl.extract import extract_employee, extract_timesheets
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Allow importing from src/etl
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../src'))


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for employee and timesheet data',
    schedule_interval=None,  # manual trigger
    start_date=datetime(2026, 3, 2),
    catchup=False
)
