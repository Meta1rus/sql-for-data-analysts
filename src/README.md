# Исходный код (`src/`)

Пакет разделён на три модуля: генерация синтетических данных для 5 источников, коннекторы/хендлеры БД (включая `PySparkHandler`) и логика загрузки слоёв DWH.

| Пакет / модуль | Назначение |
|---|---|
| [`data_generation/constants.py`](data_generation/constants.py), [`utils.py`](data_generation/utils.py) | Константы (часовые пояса, стадии заказов, кластеры, горизонты прогнозов) и генераторы случайных распределений |
| [`data_generation/pg_prod.py`](data_generation/pg_prod.py) | Генерация датафреймов `products`, `shops`, `users` для `postgres_main` |
| [`data_generation/pg_ml.py`](data_generation/pg_ml.py) | Генерация датафреймов ML-прогнозов заказов и выручки (`catboost`, `TRex`) для `postgres_ml` |
| [`data_generation/clickhouse.py`](data_generation/clickhouse.py) | Генерация `interactions`, `orders_history`, `products_price_history`, `delivery_price_history` |
| [`data_generation/mongodb.py`](data_generation/mongodb.py) | Генерация истории парсинга цен конкурентов `competitors_products_price_history` |
| [`data_generation/redis.py`](data_generation/redis.py) | Генерация городов (`users_cities`) и ценовых кластеров (`users_clusters`) пользователей |
| [`data_handlers/config.py`](data_handlers/config.py) | Pydantic-конфигурации `PostgresConfig`, `ClickhouseConfig`, `MongoConfig`, `RedisConfig`, `SparkConfig` |
| [`data_handlers/pg_handler.py`](data_handlers/pg_handler.py) | Хендлер PostgreSQL (SQLAlchemy + `psycopg` v3, пакетная вставка с защитой от лимита 65 535 параметров) |
| [`data_handlers/clickhouse_handler.py`](data_handlers/clickhouse_handler.py) | Хендлер ClickHouse (`clickhouse-connect`, создание БД и таблиц `MergeTree` по схеме DataFrame) |
| [`data_handlers/mongodb_handler.py`](data_handlers/mongodb_handler.py) | Хендлер MongoDB (`pymongo`, пакетная вставка документов с приведением `UUID` и `date`) |
| [`data_handlers/redis_handler.py`](data_handlers/redis_handler.py) | Хендлер Redis (`redis-py`, чтение и запись HASH-ключей через pipeline) |
| [`data_handlers/spark_handler.py`](data_handlers/spark_handler.py) | **`PySparkHandler` (Задание 2):** чтение из PostgreSQL, ClickHouse, MongoDB, Redis и запись в DWH через PySpark JDBC |
| [`dwh/raw_layer_schemas.py`](dwh/raw_layer_schemas.py) | Реестр 13 таблиц `raw_layer` (режимы `truncate` и `incremental`, колонки watermark) |
| [`dwh/raw_loader.py`](dwh/raw_loader.py) | **`RawLayerLoader` (Задание 2):** полная перезапись справочников и инкрементальная дозапись таблиц фактов в `raw_layer` |
| [`dwh/sql_runner.py`](dwh/sql_runner.py) | Исполнитель SQL-скриптов слоёв `stage_layer` и `data_marts` через `PySparkHandler` |
| [`dwh/marts_builder.py`](dwh/marts_builder.py) | Динамический построитель широкой pivot-витрины `data_marts.user_product_orders_last_4m` по всем `product_id` (Задание 5.4) |
