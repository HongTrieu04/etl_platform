"""DAG: Oracle → Raw zone migration.

Schedule: Daily at 02:00
Triggers extraction of Oracle source tables into MinIO raw zone.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="migration_oracle_to_raw",
    default_args=default_args,
    description="Extract Oracle source tables into MinIO Raw zone (Parquet)",
    schedule="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["migration", "oracle", "raw"],
) as dag:

    run_migration = SparkSubmitOperator(
        task_id="spark_oracle_to_raw",
        application="/opt/spark/jobs/migration/oracle_to_raw.py",
        conn_id="spark_default",
        application_args=[
            "--etl-date", "{{ ds_nodash }}",
            "--run-type", "FULL",
        ],
        verbose=True,
    )
