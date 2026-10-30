from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.dwh.sql_runner import execute_sql_file

DEFAULT_ARGS = {
    "owner": "filipp_podolskii",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="stage_orders_full",
    default_args=DEFAULT_ARGS,
    description="Задание 4.4: Сборка полной витрины заказов stage_layer.orders_full",
    schedule="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    tags=["dwh", "stage_layer", "task4"],
) as dag:
    build_stage_orders_full = PythonOperator(
        task_id="build_stage_orders_full",
        python_callable=execute_sql_file,
        op_kwargs={"sql_file": "sql/stage_layer/04_orders_full.sql"},
    )
