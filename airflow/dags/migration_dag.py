from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id="migration_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["migration", "oracle", "raw"],
) as dag:
    start = EmptyOperator(task_id="start")
    run_migration = EmptyOperator(task_id="run_oracle_to_raw")
    end = EmptyOperator(task_id="end")

    start >> run_migration >> end
