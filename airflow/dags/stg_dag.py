from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id="stg_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["stg", "transform"],
) as dag:
    start = EmptyOperator(task_id="start")
    run_stg = EmptyOperator(task_id="run_raw_to_stg")
    end = EmptyOperator(task_id="end")

    start >> run_stg >> end
