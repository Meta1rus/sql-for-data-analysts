from __future__ import annotations

import logging
import os

from dotenv import load_dotenv

from src.data_handlers.config import (
    ClickhouseConfig,
    MongoConfig,
    PostgresConfig,
    RedisConfig,
)
from src.data_handlers.spark_handler import PySparkHandler

load_dotenv()
logger = logging.getLogger(__name__)


class RawLayerLoader:
    """Загрузчик данных из операционных источников в слой raw_layer DWH через PySpark."""

    def __init__(self, spark_handler: PySparkHandler) -> None:
        self.spark_handler = spark_handler
        self.pg_main_config = PostgresConfig(
            host=os.getenv("POSTGRES_MAIN_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_MAIN_PORT", "5432")),
            dbname=os.getenv("POSTGRES_MAIN_DB", "postgres"),
            user=os.getenv("POSTGRES_MAIN_USER", "postgres"),
            password=os.getenv("POSTGRES_MAIN_PASSWORD", "postgres"),
        )
        self.pg_ml_config = PostgresConfig(
            host=os.getenv("POSTGRES_ML_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_ML_PORT", "5433")),
            dbname=os.getenv("POSTGRES_ML_DB", "postgres"),
            user=os.getenv("POSTGRES_ML_USER", "postgres"),
            password=os.getenv("POSTGRES_ML_PASSWORD", "postgres"),
        )
        self.ch_source_config = ClickhouseConfig(
            host=os.getenv("CLICKHOUSE_HOST", "localhost"),
            port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
            username=os.getenv("CLICKHOUSE_USER", "default"),
            password=os.getenv("CLICKHOUSE_PASSWORD", "clickhouse"),
            database=os.getenv("CLICKHOUSE_DB", "default"),
        )
        self.ch_dwh_config = ClickhouseConfig(
            host=os.getenv("CLICKHOUSE_DWH_HOST", "localhost"),
            port=int(os.getenv("CLICKHOUSE_DWH_PORT", "8124")),
            username=os.getenv("CLICKHOUSE_DWH_USER", "default"),
            password=os.getenv("CLICKHOUSE_DWH_PASSWORD", "clickhouse_dwh"),
            database=os.getenv("CLICKHOUSE_DWH_DB", "raw_layer"),
        )
        self.mongo_config = MongoConfig(
            host=os.getenv("MONGO_HOST", "localhost"),
            port=int(os.getenv("MONGO_PORT", "27017")),
            username=os.getenv("MONGO_USER", "mongo"),
            password=os.getenv("MONGO_PASSWORD", "mongo"),
            database=os.getenv("MONGO_DB", "shop"),
        )
        self.redis_config = RedisConfig(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            db=int(os.getenv("REDIS_DB", "0")),
            password=os.getenv("REDIS_PASSWORD", "redis"),
        )

    def sync_truncate_table(self, table_name: str, source: str) -> None:
        """Полная перезапись справочника в raw_layer через TRUNCATE + PySpark write."""
        logger.info("Полная перезагрузка (TRUNCATE) таблицы raw_layer.%s из %s", table_name, source)
        if source == "pg_main":
            df = self.spark_handler.read_postgres(self.pg_main_config, table_name)
        elif source == "pg_ml":
            df = self.spark_handler.read_postgres(self.pg_ml_config, table_name)
        elif source == "redis":
            df = self.spark_handler.read_redis_hashes(
                self.redis_config, table_prefix=table_name, key_column="user_id"
            )
        elif source == "clickhouse":
            df = self.spark_handler.read_clickhouse(self.ch_source_config, table_name)
        elif source == "mongodb":
            df = self.spark_handler.read_mongodb(self.mongo_config, table_name)
        else:
            raise ValueError(f"Неизвестный источник: {source}")

        self.spark_handler.write_clickhouse(
            df=df,
            ch_config=self.ch_dwh_config,
            table_name=table_name,
            database="raw_layer",
            mode="overwrite",
        )
