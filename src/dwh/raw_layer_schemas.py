from __future__ import annotations

from pathlib import Path

from src.data_handlers.config import ClickhouseConfig
from src.data_handlers.spark_handler import PySparkHandler

RAW_LAYER_TABLES: dict[str, dict[str, str]] = {
    # Full reload (TRUNCATE + INSERT) tables
    "products": {
        "source": "pg_main",
        "mode": "truncate",
    },
    "shops": {
        "source": "pg_main",
        "mode": "truncate",
    },
    "users": {
        "source": "pg_main",
        "mode": "incremental",
        "watermark_col": "registration_date",
    },
    "users_cities": {
        "source": "redis",
        "mode": "truncate",
    },
    "users_clusters": {
        "source": "redis",
        "mode": "truncate",
    },
    "shops_daily_completed_orders_predict": {
        "source": "pg_ml",
        "mode": "incremental",
        "watermark_col": "creation_date",
    },
    "shops_daily_all_orders_predict": {
        "source": "pg_ml",
        "mode": "incremental",
        "watermark_col": "creation_date",
    },
    "daily_revenue_predict": {
        "source": "pg_ml",
        "mode": "incremental",
        "watermark_col": "creation_date",
    },
    # Append / incremental watermark tables
    "interactions": {
        "source": "clickhouse",
        "mode": "incremental",
        "watermark_col": "datetime",
    },
    "orders_history": {
        "source": "clickhouse",
        "mode": "incremental",
        "watermark_col": "datetime",
    },
    "products_price_history": {
        "source": "clickhouse",
        "mode": "incremental",
        "watermark_col": "effective_from",
    },
    "delivery_price_history": {
        "source": "clickhouse",
        "mode": "incremental",
        "watermark_col": "effective_from",
    },
    "competitors_products_price_history": {
        "source": "mongodb",
        "mode": "incremental",
        "watermark_col": "datetime",
    },
}


def init_dwh_schemas_and_raw_tables(
    spark_handler: PySparkHandler,
    dwh_config: ClickhouseConfig,
    sql_dir: Path = Path("sql"),
) -> None:
    for schema_name in ("raw_layer", "stage_layer", "data_marts"):
        spark_handler.execute_dwh_sql(
            dwh_config, f"CREATE DATABASE IF NOT EXISTS {schema_name}"
        )

    ddl_path = sql_dir / "raw_layer" / "01_raw_tables_ddl.sql"
    ddl_content = ddl_path.read_text(encoding="utf-8")
    for statement in ddl_content.split(";"):
        stmt = statement.strip()
        if stmt:
            spark_handler.execute_dwh_sql(dwh_config, stmt)
