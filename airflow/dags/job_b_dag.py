"""DAG: Raw → SOR transformation for Job B.

Schedule: None (triggered by ar_x_au DAG).
"""
import os
from datetime import datetime, timedelta
import socket

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

# Explicitly collect env vars to forward to the spark-submit process.
_SPARK_ENV_VARS = {
    "ORACLE_HOST": os.getenv("ORACLE_HOST", ""),
    "ORACLE_PORT": os.getenv("ORACLE_PORT", "1521"),
    "ORACLE_SERVICE_NAME": os.getenv("ORACLE_SERVICE_NAME", ""),
    "ORACLE_APP_USER": os.getenv("ORACLE_APP_USER", ""),
    "ORACLE_APP_USER_PASSWORD": os.getenv("ORACLE_APP_USER_PASSWORD", ""),
    "MINIO_ROOT_USER": os.getenv("MINIO_ROOT_USER", ""),
    "MINIO_ROOT_PASSWORD": os.getenv("MINIO_ROOT_PASSWORD", ""),
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
        jars="/opt/airflow/jars/ojdbc8.jar,/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar",
        conf={
            "spark.driver.host": socket.gethostbyname(socket.gethostname()),
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.cores.max": "2",
        },
        env_vars=_SPARK_ENV_VARS,
        application_args=[
            "--etl-date", "{{ dag_run.conf.get('etl_date', ds_nodash) }}",
            "--run-type", "FULL",
        ],
        verbose=True,
    )

