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


def load_data_to_databases(
    databases_dfs: dict[str, list[pd.DataFrame]],
    connections: dict[str, Any],
) -> None:
    try:
        # 1. PG main
        pg_main: PostgresHandler = connections["pg_main"]
        pg_main_tables = ["products", "shops", "users"]
        for table_name, df in zip(pg_main_tables, databases_dfs["pg_main"]):
            logger.info("Loading %s into PG main (%d rows)...", table_name, len(df))
            pg_main.load_dataframe(df, table_name=table_name, mode="replace")

        # 2. PG ML
        pg_ml: PostgresHandler = connections["pg_ml"]
        pg_ml_tables = [
            "shops_daily_completed_orders_predict",
            "shops_daily_all_orders_predict",
            "daily_revenue_predict",
        ]
        for table_name, df in zip(pg_ml_tables, databases_dfs["pg_ml"]):
            logger.info("Loading %s into PG ML (%d rows)...", table_name, len(df))
            pg_ml.load_dataframe(df, table_name=table_name, mode="replace")

        # 3. ClickHouse
        ch: ClickHouseHandler = connections["clickhouse"]
        ch.create_database()
        ch_specs = [
            ("interactions", "interaction_id"),
            ("orders_history", "(order_id, datetime)"),
            ("products_price_history", "(product_id, effective_from)"),
            ("delivery_price_history", "(object_type, object_id, effective_from)"),
        ]
        for (table_name, order_by), df in zip(ch_specs, databases_dfs["clickhouse"]):
            logger.info("Loading %s into ClickHouse (%d rows)...", table_name, len(df))
            ch.create_table_from_dataframe(df, table_name=table_name, order_by=order_by)
            ch.load_dataframe(df, table_name=table_name, mode="replace")

        # 4. MongoDB
        mongo: MongoHandler = connections["mongodb"]
        comp_df = databases_dfs["mongodb"][0]
        logger.info(
            "Loading competitors_products_price_history into MongoDB (%d rows)...",
            len(comp_df),
        )
        mongo.load_dataframe(
            comp_df,
            collection_name="competitors_products_price_history",
            mode="replace",
        )

        # 5. Redis
        redis_handler: RedisHandler = connections["redis"]
        redis_handler.flush_database()
        users_cities_df, users_clusters_df = databases_dfs["redis"]
        logger.info(
            "Loading users_cities into Redis (%d keys)...", len(users_cities_df)
        )
        redis_handler.load_dataframe_as_hashes(
            users_cities_df,
            key_template="users_cities:{user_id}",
            key_fields=["user_id"],
            value_fields=["city"],
        )
        logger.info(
            "Loading users_clusters into Redis (%d keys)...", len(users_clusters_df)
        )
        redis_handler.load_dataframe_as_hashes(
            users_clusters_df,
            key_template="users_clusters:{user_id}",
            key_fields=["user_id"],
            value_fields=["price_cluster"],
        )
        logger.info("All 13 source tables initialized and loaded successfully.")
    finally:
        for handler in connections.values():
            handler.close()


if __name__ == "__main__":
    databases_dfs = init_databases_dataframes()
    connections = init_databases_connections()
    load_data_to_databases(databases_dfs, connections)
