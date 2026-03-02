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

extract_employee_task = PythonOperator(
    task_id='extract_employee',
    python_callable=extract_employee,
    dag=dag
)

extract_timesheets_task = PythonOperator(
    task_id='extract_timesheets',
    python_callable=extract_timesheets,
    dag=dag
)

transform_employee_task = PythonOperator(
    task_id='transform_employee',
    python_callable=transform_employee,
    dag=dag
)

transform_timesheet_task = PythonOperator(
    task_id='transform_timesheet',
    python_callable=transform_timesheet,
    dag=dag
)

load_employee_task = PythonOperator(
    task_id='load_employee',
    python_callable=load_employee,
    dag=dag
)

load_timesheet_task = PythonOperator(
    task_id='load_timesheet',
    python_callable=load_timesheet,
    dag=dag
)

persist_derived_task = PythonOperator(
    task_id='persist_derived_timesheet',
    python_callable=persist_derived_timesheet,
    dag=dag
)

# Employee flow
extract_employee_task >> transform_employee_task >> load_employee_task

# Timesheet flow
extract_timesheets_task >> transform_timesheet_task >> load_timesheet_task

# Derived table depends on both loads
[load_employee_task, load_timesheet_task] >> persist_derived_task
