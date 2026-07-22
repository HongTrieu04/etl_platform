"""DAG: Raw → SOR transformation for AR_X_AU.

Schedule: Daily at 03:00
After completion, triggers Job B DAG.
"""
from datetime import datetime, timedelta
import socket

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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
    dag_id="sor_ar_x_au",
    default_args=default_args,
    description="Transform Raw TBAADM_GAM_TDY into SOR AR_X_AU (Parquet)",
    schedule="0 3 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["sor", "ar_x_au", "transformation"],
) as dag:

    run_ar_x_au = SparkSubmitOperator(
        task_id="spark_ar_x_au",
        application="/opt/spark/jobs/sor/ar_x_au.py",
        conn_id="spark_default",
        conf={
            "spark.driver.host": socket.gethostbyname(socket.gethostname()),
            "spark.driver.bindAddress": "0.0.0.0",
        },
        application_args=[
            "--etl-date", "{{ ds_nodash }}",
            "--run-type", "FULL",
        ],
        verbose=True,
    )

    trigger_job_b = TriggerDagRunOperator(
        task_id="trigger_job_b",
        trigger_dag_id="sor_job_b",
        conf={"etl_date": "{{ ds_nodash }}"},
        wait_for_completion=False,
    )

    run_ar_x_au >> trigger_job_b
