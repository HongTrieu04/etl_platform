from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id="reconcile_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["reconcile", "quality"],
) as dag:
    start = EmptyOperator(task_id="start")
    run_reconcile = EmptyOperator(task_id="run_sor_reconcile")
    end = EmptyOperator(task_id="end")

    start >> run_reconcile >> end
