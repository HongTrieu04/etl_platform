from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id="sor_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["sor", "warehouse"],
) as dag:
    start = EmptyOperator(task_id="start")
    run_sor = EmptyOperator(task_id="run_stg_to_sor")
    end = EmptyOperator(task_id="end")

    start >> run_sor >> end
