import logging
import os
from typing import Any

import pandas as pd
from dotenv import load_dotenv

from src.data_generation.clickhouse import (
    gen_delivery_price_history_dataframe,
    gen_products_price_history_dataframe,
    generate_interaction_history_dataframe,
    generate_orders_history_dataframe,
)
from src.data_generation.mongodb import gen_competitors_products_price_history_dataframe
from src.data_generation.pg_ml import (
    gen_daily_revenue_predict,
    gen_shops_daily_all_orders_predict,
    gen_shops_daily_completed_orders_predict,
)
from src.data_generation.pg_prod import (
    generate_products_dataframe,
    generate_shops_dataframe,
    generate_users_dataframe,
)
from src.data_generation.redis import (
    gen_users_cities_dataframe,
    gen_users_clusters_dataframe,
)
from src.data_handlers.clickhouse_handler import ClickHouseHandler
from src.data_handlers.config import (
    ClickhouseConfig,
    MongoConfig,
    PostgresConfig,
    RedisConfig,
)
from src.data_handlers.mongodb_handler import MongoHandler
from src.data_handlers.pg_handler import PostgresHandler
from src.data_handlers.redis_handler import RedisHandler

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

UNIQUE_USERS_NUM = int(os.getenv("UNIQUE_USERS_NUM", "10000"))
UNIQUE_SHOPS_NUM = int(os.getenv("UNIQUE_SHOPS_NUM", "500"))
NUM_UNIQUE_INTERACTIONS = int(os.getenv("NUM_UNIQUE_INTERACTIONS", "200000"))


def init_databases_dataframes() -> dict[str, list[pd.DataFrame]]:
    # PG prod
    products_df = generate_products_dataframe(pd.read_csv("data/products.csv", sep=";"))
    shops_df = generate_shops_dataframe(UNIQUE_SHOPS_NUM)
    users_df = generate_users_dataframe(UNIQUE_USERS_NUM)

    # Clickhouse
    interactions_df = generate_interaction_history_dataframe(
        products_df.copy(), users_df.copy(), NUM_UNIQUE_INTERACTIONS
    )
    orders_history = generate_orders_history_dataframe(
        interactions_df.copy(), shops_df.copy()
    )
    products_price_history_df = gen_products_price_history_dataframe(products_df.copy())
    delivery_price_history_df = gen_delivery_price_history_dataframe(shops_df.copy())

    # Mongodb
    competitors_products_price_history_df = (
        gen_competitors_products_price_history_dataframe(products_df.copy())
    )

    # PG ml
    shops_daily_completed_orders_predict_df = gen_shops_daily_completed_orders_predict(
        shops_df.copy()
    )
    shops_daily_all_orders_predict_df = gen_shops_daily_all_orders_predict(
        shops_daily_completed_orders_predict_df.copy()
    )
    daily_revenue_predict_df = gen_daily_revenue_predict()

    # Redis
    users_cities_df = gen_users_cities_dataframe(users_df.copy())
    users_clusters_df = gen_users_clusters_dataframe(users_df.copy())
    return {
        "pg_main": [products_df, shops_df, users_df],
        "pg_ml": [
            shops_daily_completed_orders_predict_df,
            shops_daily_all_orders_predict_df,
            daily_revenue_predict_df,
        ],
        "redis": [users_cities_df, users_clusters_df],
        "mongodb": [competitors_products_price_history_df],
        "clickhouse": [
            interactions_df,
            orders_history,
            products_price_history_df,
            delivery_price_history_df,
        ],
    }


def init_databases_connections() -> dict[str, Any]:
    pg_main_config = PostgresConfig(
        host=os.getenv("POSTGRES_MAIN_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_MAIN_PORT", "5432")),
        dbname=os.getenv("POSTGRES_MAIN_DB", "postgres"),
        user=os.getenv("POSTGRES_MAIN_USER", "postgres"),
        password=os.getenv("POSTGRES_MAIN_PASSWORD", "postgres"),
    )
    pg_ml_config = PostgresConfig(
        host=os.getenv("POSTGRES_ML_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_ML_PORT", "5433")),
        dbname=os.getenv("POSTGRES_ML_DB", "postgres"),
        user=os.getenv("POSTGRES_ML_USER", "postgres"),
        password=os.getenv("POSTGRES_ML_PASSWORD", "postgres"),
    )
    clickhouse_config = ClickhouseConfig(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "clickhouse"),
        database=os.getenv("CLICKHOUSE_DB", "default"),
    )
    mongo_config = MongoConfig(
        host=os.getenv("MONGO_HOST", "localhost"),
        port=int(os.getenv("MONGO_PORT", "27017")),
        username=os.getenv("MONGO_USER", "mongo"),
        password=os.getenv("MONGO_PASSWORD", "mongo"),
        database=os.getenv("MONGO_DB", "shop"),
    )
    redis_config = RedisConfig(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        db=int(os.getenv("REDIS_DB", "0")),
        password=os.getenv("REDIS_PASSWORD", "redis"),
    )
    return {
        "pg_main": PostgresHandler(pg_main_config),
        "pg_ml": PostgresHandler(pg_ml_config),
        "clickhouse": ClickHouseHandler(clickhouse_config),
        "mongodb": MongoHandler(mongo_config),
        "redis": RedisHandler(redis_config),
    }


def load_data_to_databases():
    pass


if __name__ == "__main__":
    databases_dfs = init_databases_dataframes()
