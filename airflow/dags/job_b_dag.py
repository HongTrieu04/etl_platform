"""DAG: Raw → SOR transformation for Job B.

Schedule: None (triggered by ar_x_au DAG).
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
    dag_id="sor_job_b",
    default_args=default_args,
    description="Transform Raw data into SOR (Job B) — triggered by ar_x_au",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["sor", "job_b", "transformation"],
) as dag:

    run_job_b = SparkSubmitOperator(
        task_id="spark_job_b",
        application="/opt/spark/jobs/sor/job_b.py",
        conn_id="spark_default",
        conf={
            "spark.driver.host": "airflow-scheduler",
            "spark.driver.bindAddress": "0.0.0.0",
        },
        application_args=[
            "--etl-date", "{{ dag_run.conf.get('etl_date', ds_nodash) }}",
            "--run-type", "FULL",
        ],
        verbose=True,
    )
