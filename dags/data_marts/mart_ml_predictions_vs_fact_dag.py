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
    dag_id="mart_ml_predictions_vs_fact",
    default_args=DEFAULT_ARGS,
    description="Задание 5.5: Сравнение ML-прогнозов заказов и выручки с фактическими данными",
    schedule="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    tags=["dwh", "data_marts", "task5"],
) as dag:
    build_mart_ml_vs_fact = PythonOperator(
        task_id="build_mart_ml_predictions_vs_fact",
        python_callable=execute_sql_file,
        op_kwargs={"sql_file": "sql/data_marts/05_ml_predictions_vs_fact.sql"},
    )
