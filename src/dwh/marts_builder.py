from __future__ import annotations

import logging

from src.data_handlers.spark_handler import PySparkHandler
from src.dwh.sql_runner import get_dwh_config

logger = logging.getLogger(__name__)


def build_user_product_orders_pivot_mart() -> None:
    """Динамическая сборка wide-витрины data_marts.user_product_orders_last_4m
    (customer_id + отдельная колонка для каждого product_id за последние 4 месяца)."""
    dwh_config = get_dwh_config()

    with PySparkHandler() as spark_handler:
        products_pdf = spark_handler.query_dwh_pandas(
            dwh_config,
            "SELECT DISTINCT product_id FROM raw_layer.products ORDER BY product_id",
        )
        product_ids = [int(pid) for pid in products_pdf["product_id"].tolist()]
        if not product_ids:
            product_ids = list(range(1001, 2001))

        col_defs = ",\n    ".join(f'"product_{pid}" UInt32' for pid in product_ids)
        pivot_exprs = ",\n    ".join(
            f'toUInt32(countDistinctIf(order_id, product_id = {pid})) AS "product_{pid}"'
            for pid in product_ids
        )

        spark_handler.execute_dwh_sql(
            dwh_config, "CREATE DATABASE IF NOT EXISTS data_marts"
        )
        spark_handler.execute_dwh_sql(
            dwh_config, "DROP TABLE IF EXISTS data_marts.user_product_orders_last_4m"
        )
        create_sql = f"""
        CREATE TABLE data_marts.user_product_orders_last_4m (
            customer_id String,
            {col_defs}
        ) ENGINE = MergeTree()
        ORDER BY customer_id
        """
        spark_handler.execute_dwh_sql(dwh_config, create_sql)

        insert_sql = f"""
        INSERT INTO data_marts.user_product_orders_last_4m
        WITH max_dt AS (
            SELECT max(toDate(created_at)) AS max_order_date
            FROM stage_layer.orders_full
        )
        SELECT
            user_id AS customer_id,
            {pivot_exprs}
        FROM stage_layer.orders_full
        WHERE created_at IS NOT NULL
          AND toDate(created_at) >= subtractMonths((SELECT max_order_date FROM max_dt), 4)
        GROUP BY customer_id
        """
        spark_handler.execute_dwh_sql(dwh_config, insert_sql)
        logger.info(
            "Построена wide-витрина data_marts.user_product_orders_last_4m (%d колонок товаров)",
            len(product_ids),
        )
