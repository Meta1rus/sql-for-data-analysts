from __future__ import annotations

import logging
import os

from dotenv import load_dotenv
from pyspark.sql import functions as F

from src.data_handlers.config import (
    ClickhouseConfig,
    MongoConfig,
    PostgresConfig,
    RedisConfig,
)
from src.data_handlers.spark_handler import PySparkHandler
from src.dwh.raw_layer_schemas import (
    RAW_LAYER_TABLES,
    init_dwh_schemas_and_raw_tables,
)

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

    def _get_dwh_watermark(self, table_name: str, watermark_col: str) -> str | None:
        query = (
            f"SELECT count() AS cnt, toString(max({watermark_col})) AS max_val "
            f"FROM raw_layer.{table_name}"
        )
        pdf = self.spark_handler.query_dwh_pandas(self.ch_dwh_config, query)
        if pdf.empty or int(pdf.iloc[0]["cnt"]) == 0:
            return None
        max_val = pdf.iloc[0]["max_val"]
        return str(max_val) if max_val is not None else None

    def sync_incremental_table(
        self, table_name: str, source: str, watermark_col: str
    ) -> None:
        """Инкрементальная дозапись (append по watermark) для больших таблиц фактов и историй."""
        watermark = self._get_dwh_watermark(table_name, watermark_col)
        logger.info(
            "Инкрементальная дозапись raw_layer.%s из %s (watermark %s=%s)",
            table_name,
            source,
            watermark_col,
            watermark,
        )

        if source in {"pg_main", "pg_ml"}:
            pg_cfg = self.pg_main_config if source == "pg_main" else self.pg_ml_config
            if watermark:
                sql = (
                    f"SELECT * FROM public.{table_name} "
                    f"WHERE {watermark_col} > '{watermark}'"
                )
                df = self.spark_handler.read_postgres(pg_cfg, sql, is_query=True)
            else:
                df = self.spark_handler.read_postgres(pg_cfg, table_name)
        elif source == "clickhouse":
            if watermark:
                sql = (
                    f"SELECT * FROM {self.ch_source_config.database}.{table_name} "
                    f"WHERE {watermark_col} > parseDateTime64BestEffort('{watermark}', 6, 'UTC')"
                )
                df = self.spark_handler.read_clickhouse(
                    self.ch_source_config, sql, is_query=True
                )
            else:
                df = self.spark_handler.read_clickhouse(
                    self.ch_source_config, table_name
                )
        elif source == "mongodb":
            df = self.spark_handler.read_mongodb(self.mongo_config, table_name)
            if watermark:
                df = df.filter(F.col(watermark_col) > F.lit(watermark))
        else:
            raise ValueError(f"Источник {source} не поддерживает инкрементальную загрузку")

        if df.isEmpty():
            logger.info("Нет новых записей для raw_layer.%s", table_name)
            return

        self.spark_handler.write_clickhouse(
            df=df,
            ch_config=self.ch_dwh_config,
            table_name=table_name,
            database="raw_layer",
            mode="append",
        )

    def sync_all_raw_tables(self, force_full: bool = False) -> None:
        init_dwh_schemas_and_raw_tables(self.spark_handler, self.ch_dwh_config)
        for table_name, spec in RAW_LAYER_TABLES.items():
            source = spec["source"]
            mode = spec["mode"]
            if force_full or mode == "truncate":
                self.sync_truncate_table(table_name=table_name, source=source)
            else:
                self.sync_incremental_table(
                    table_name=table_name,
                    source=source,
                    watermark_col=spec["watermark_col"],
                )

