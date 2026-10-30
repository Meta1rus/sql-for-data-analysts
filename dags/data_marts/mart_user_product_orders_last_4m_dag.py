from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.dwh.marts_builder import build_user_product_orders_pivot_mart

DEFAULT_ARGS = {
    "owner": "filipp_podolskii",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="mart_user_product_orders_last_4m",
    default_args=DEFAULT_ARGS,
    description="Задание 5.4: Витрина заказов пользователей по каждому товару за последние 4 месяца",
    schedule="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    tags=["dwh", "data_marts", "task5"],
) as dag:
    build_mart_user_products = PythonOperator(
        task_id="build_mart_user_product_orders_last_4m",
        python_callable=build_user_product_orders_pivot_mart,
    )
