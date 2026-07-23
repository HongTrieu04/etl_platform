"""DAG: Reconciliation check for AR_X_AU (Oracle vs MinIO SOR).

Schedule: None (triggered manually or after sor_ar_x_au).
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
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

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
    dag_id="reconcile_dag",
    default_args=default_args,
    description="Reconcile data quality between Oracle and MinIO SOR (AR_X_AU)",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["reconcile", "quality", "ar_x_au"],
) as dag:

    run_reconcile = SparkSubmitOperator(
        task_id="spark_reconcile_ar_x_au",
        application="/opt/spark/jobs/reconcile/reconcile_ar_x_au.py",
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
