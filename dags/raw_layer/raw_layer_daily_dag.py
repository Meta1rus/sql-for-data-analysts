from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.data_handlers.spark_handler import PySparkHandler
from src.dwh.raw_layer_schemas import (
    RAW_LAYER_TABLES,
    init_dwh_schemas_and_raw_tables,
)
from src.dwh.raw_loader import RawLayerLoader

DEFAULT_ARGS = {
    "owner": "filipp_podolskii",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _init_raw_schemas() -> None:
    with PySparkHandler() as spark_handler:
        loader = RawLayerLoader(spark_handler)
        init_dwh_schemas_and_raw_tables(spark_handler, loader.ch_dwh_config)


def _sync_single_table(table_name: str) -> None:
    spec = RAW_LAYER_TABLES[table_name]
    with PySparkHandler() as spark_handler:
        loader = RawLayerLoader(spark_handler)
        if spec["mode"] == "truncate":
            loader.sync_truncate_table(table_name, spec["source"])
        else:
            loader.sync_incremental_table(
                table_name, spec["source"], spec["watermark_col"]
            )


with DAG(
    dag_id="raw_layer_daily_sync",
    default_args=DEFAULT_ARGS,
    description="Ежедневное обновление таблиц слоя raw_layer из 5 источников через PySpark",
    schedule="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    tags=["dwh", "raw_layer", "pyspark"],
) as dag:
    init_schemas_task = PythonOperator(
        task_id="init_raw_layer_schemas",
        python_callable=_init_raw_schemas,
    )

    for tbl_name in RAW_LAYER_TABLES:
        sync_task = PythonOperator(
            task_id=f"sync_raw_{tbl_name}",
            python_callable=_sync_single_table,
            op_kwargs={"table_name": tbl_name},
        )
        init_schemas_task >> sync_task
