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
    dag_id="mart_daily_competitors_price_diff",
    default_args=DEFAULT_ARGS,
    description="Задание 5.1: Средняя разница цен с конкурентами за день",
    schedule="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    tags=["dwh", "data_marts", "task5"],
) as dag:
    build_mart_price_diff = PythonOperator(
        task_id="build_mart_daily_competitors_price_diff",
        python_callable=execute_sql_file,
        op_kwargs={"sql_file": "sql/data_marts/01_daily_competitors_price_diff.sql"},
    )
