import logging

from src.dwh.marts_builder import build_user_product_orders_pivot_mart
from src.dwh.sql_runner import execute_sql_file

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

STAGE_SQL_FILES = [
    "sql/stage_layer/01_competitors_price_history.sql",
    "sql/stage_layer/02_ml_predictions.sql",
    "sql/stage_layer/03_daily_interactions_by_type.sql",
    "sql/stage_layer/04_orders_full.sql",
]

MART_SQL_FILES = [
    "sql/data_marts/01_daily_competitors_price_diff.sql",
    "sql/data_marts/02_monthly_orders_by_cluster_city.sql",
    "sql/data_marts/03_yearly_avg_weekly_conversion.sql",
    "sql/data_marts/05_ml_predictions_vs_fact.sql",
]


def main() -> None:
    logger.info("Сборка витрин слоя stage_layer (Задание 4)...")
    for sql_file in STAGE_SQL_FILES:
        execute_sql_file(sql_file)

    logger.info("Сборка витрин слоя data_marts (Задание 5)...")
    for sql_file in MART_SQL_FILES:
        execute_sql_file(sql_file)
    build_user_product_orders_pivot_mart()
    logger.info("Все витрины stage_layer и data_marts успешно собраны.")


if __name__ == "__main__":
    main()
