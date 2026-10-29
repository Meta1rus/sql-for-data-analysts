from __future__ import annotations

import logging
from typing import Any

import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession

from src.data_handlers.clickhouse_handler import ClickHouseHandler
from src.data_handlers.config import (
    ClickhouseConfig,
    MongoConfig,
    PostgresConfig,
    RedisConfig,
    SparkConfig,
)
from src.data_handlers.mongodb_handler import MongoHandler
from src.data_handlers.redis_handler import RedisHandler

logger = logging.getLogger(__name__)

POSTGRES_JDBC_DRIVER = "org.postgresql.Driver"
CLICKHOUSE_JDBC_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver"


class PySparkHandler:
    """Хендлер для чтения из источников и записи в DWH с помощью PySpark."""

    def __init__(self, config: SparkConfig | None = None) -> None:
        self.config = config or SparkConfig()
        self._spark: SparkSession | None = None

    @property
    def spark(self) -> SparkSession:
        if self._spark is None:
            builder = (
                SparkSession.builder.appName(self.config.app_name)
                .master(self.config.master)
                .config("spark.driver.memory", self.config.driver_memory)
                .config("spark.executor.memory", self.config.executor_memory)
                .config(
                    "spark.sql.shuffle.partitions",
                    str(self.config.shuffle_partitions),
                )
                .config("spark.sql.session.timeZone", "UTC")
            )
            if self.config.packages:
                builder = builder.config(
                    "spark.jars.packages", ",".join(self.config.packages)
                )
            self._spark = builder.getOrCreate()
        return self._spark

    def close(self) -> None:
        if self._spark is not None:
            self._spark.stop()
            self._spark = None

    def __enter__(self) -> PySparkHandler:
        _ = self.spark
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    def read_postgres(
        self,
        pg_config: PostgresConfig,
        table_or_query: str,
        is_query: bool = False,
        fetchsize: int = 10_000,
    ) -> SparkDataFrame:
        reader = (
            self.spark.read.format("jdbc")
            .option("url", pg_config.build_jdbc_url())
            .option("user", pg_config.user)
            .option("password", pg_config.password)
            .option("driver", POSTGRES_JDBC_DRIVER)
            .option("fetchsize", str(fetchsize))
        )
        if is_query:
            reader = reader.option("query", table_or_query)
        else:
            reader = reader.option("dbtable", table_or_query)
        return reader.load()

    def read_clickhouse(
        self,
        ch_config: ClickhouseConfig,
        table_or_query: str,
        database: str | None = None,
        is_query: bool = False,
        fetchsize: int = 50_000,
    ) -> SparkDataFrame:
        reader = (
            self.spark.read.format("jdbc")
            .option("url", ch_config.build_jdbc_url(database=database))
            .option("user", ch_config.username)
            .option("password", ch_config.password)
            .option("driver", CLICKHOUSE_JDBC_DRIVER)
            .option("fetchsize", str(fetchsize))
        )
        if is_query:
            reader = reader.option("query", table_or_query)
        else:
            db = database or ch_config.database
            qualified = (
                table_or_query
                if "." in table_or_query
                else f"{db}.{table_or_query}"
            )
            reader = reader.option("dbtable", qualified)
        return reader.load()

    def read_mongodb(
        self,
        mongo_config: MongoConfig,
        collection_name: str,
        query: dict[str, Any] | None = None,
    ) -> SparkDataFrame:
        try:
            reader = (
                self.spark.read.format("mongodb")
                .option("spark.mongodb.read.connection.uri", mongo_config.build_uri())
                .option("spark.mongodb.read.database", mongo_config.database)
                .option("spark.mongodb.read.collection", collection_name)
            )
            df = reader.load()
            if "_id" in df.columns:
                df = df.drop("_id")
            return df
        except Exception as exc:
            logger.info(
                "Fallback чтения коллекции %s через MongoHandler -> SparkDataFrame (%s)",
                collection_name,
                exc,
            )
            with MongoHandler(mongo_config) as handler:
                pdf = handler.find_dataframe(
                    collection_name=collection_name,
                    query=query,
                    projection={"_id": 0},
                )
            return self.spark.createDataFrame(pdf)

    def read_redis_hashes(
        self,
        redis_config: RedisConfig,
        table_prefix: str,
        key_column: str = "user_id",
    ) -> SparkDataFrame:
        try:
            return (
                self.spark.read.format("org.apache.spark.sql.redis")
                .option("host", redis_config.host)
                .option("port", str(redis_config.port))
                .option("auth", redis_config.password or "")
                .option("dbNum", str(redis_config.db))
                .option("table", table_prefix)
                .option("key.column", key_column)
                .load()
            )
        except Exception as exc:
            logger.info(
                "Fallback чтения префикса %s:* через RedisHandler -> SparkDataFrame (%s)",
                table_prefix,
                exc,
            )
            with RedisHandler(redis_config) as handler:
                pdf = handler.read_hashes_to_dataframe(key_pattern=f"{table_prefix}:*")
            if not pdf.empty and "_redis_key" in pdf.columns:
                pdf[key_column] = pdf["_redis_key"].str.split(":", n=1).str[1]
                pdf = pdf.drop(columns=["_redis_key"])
            return self.spark.createDataFrame(pdf)

    def write_clickhouse(
        self,
        df: SparkDataFrame,
        ch_config: ClickhouseConfig,
        table_name: str,
        database: str | None = None,
        mode: str = "append",
        batchsize: int = 50_000,
    ) -> None:
        if mode not in {"append", "overwrite"}:
            raise ValueError("mode должен быть 'append' или 'overwrite'")

        db = database or ch_config.database
        if mode == "overwrite":
            with ClickHouseHandler(ch_config) as ch_admin:
                ch_admin.truncate_table(table_name=table_name, database=db)
            write_mode = "append"
        else:
            write_mode = "append"

        (
            df.write.format("jdbc")
            .option("url", ch_config.build_jdbc_url(database=db))
            .option("dbtable", f"{db}.{table_name}")
            .option("user", ch_config.username)
            .option("password", ch_config.password)
            .option("driver", CLICKHOUSE_JDBC_DRIVER)
            .option("batchsize", str(batchsize))
            .option("isolationLevel", "NONE")
            .mode(write_mode)
            .save()
        )
        logger.info("Записан SparkDataFrame в %s.%s (mode=%s)", db, table_name, mode)

    def execute_dwh_sql(
        self,
        ch_config: ClickhouseConfig,
        sql_query: str,
        parameters: dict[str, Any] | None = None,
    ) -> None:
        with ClickHouseHandler(ch_config) as ch_admin:
            ch_admin.execute(sql_query, parameters=parameters)

    def query_dwh_df(
        self,
        ch_config: ClickhouseConfig,
        sql_query: str,
        database: str | None = None,
    ) -> SparkDataFrame:
        return self.read_clickhouse(
            ch_config=ch_config,
            table_or_query=sql_query,
            database=database,
            is_query=True,
        )

    def query_dwh_pandas(
        self,
        ch_config: ClickhouseConfig,
        sql_query: str,
    ) -> pd.DataFrame:
        with ClickHouseHandler(ch_config) as ch_admin:
            return ch_admin.query_df(sql_query)
