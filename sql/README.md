# SQL-скрипты хранилища данных (ClickHouse DWH)

Все SQL-скрипты разделены по трём слоям хранилища `clickhouse_dwh` (`raw_layer`, `stage_layer`, `data_marts`) и выполняются через [`src/dwh/sql_runner.py`](../src/dwh/sql_runner.py) и [`PySparkHandler`](../src/data_handlers/spark_handler.py).

## Навигация по слоям и заданиям

| Слой | Задание | Файл | Целевые таблицы в ClickHouse DWH | Описание |
|---|---|---|---|---|
| **Raw layer** | Задание 2 | [`raw_layer/00_init_schemas.sql`](raw_layer/00_init_schemas.sql) | Схемы `raw_layer`, `stage_layer`, `data_marts` | Автоматическая инициализация баз данных при старте контейнера `clickhouse_dwh` |
| **Raw layer** | Задание 2 | [`raw_layer/01_raw_tables_ddl.sql`](raw_layer/01_raw_tables_ddl.sql) | 13 таблиц `raw_layer.*` | DDL всех 13 таблиц источников (`MergeTree`) |
| **Stage layer** | Задание 4.1 | [`stage_layer/01_competitors_price_history.sql`](stage_layer/01_competitors_price_history.sql) | `stage_layer.competitors_price_history` | Очищенная история цен у конкурентов (`coalesce(price, cost, value)`) в локальном времени |
| **Stage layer** | Задание 4.2 | [`stage_layer/02_ml_predictions.sql`](stage_layer/02_ml_predictions.sql) | `stage_layer.ml_completed_orders_predict`<br>`stage_layer.ml_all_orders_predict`<br>`stage_layer.ml_revenue_predict` | Последние созданные (`argMax(..., creation_date)`) ML-прогнозы завершённых заказов, всех заказов и выручки (`catboost`, `TRex`) |
| **Stage layer** | Задание 4.3 | [`stage_layer/03_daily_interactions_by_type.sql`](stage_layer/03_daily_interactions_by_type.sql) | `stage_layer.daily_interactions_by_type` | Ежедневное количество интеракций по типам в локальном времени (`delivered` маппится в `completed`) |
| **Stage layer** | Задание 4.4 | [`stage_layer/04_orders_full.sql`](stage_layer/04_orders_full.sql) | `stage_layer.orders_full` | Широкая витрина заказов: все 11 стадий в локальном часовом поясе магазина, статус (`завершен`, `в процессе`, `отменен/потерян`), `ASOF LEFT JOIN` цен товара и доставки |
| **Data Marts** | Задание 5.1 | [`data_marts/01_daily_competitors_price_diff.sql`](data_marts/01_daily_competitors_price_diff.sql) | `data_marts.daily_competitors_price_diff` | Средняя разница между ценой товара нашей компании и конкурентов за день |
| **Data Marts** | Задание 5.2 | [`data_marts/02_monthly_orders_by_cluster_city.sql`](data_marts/02_monthly_orders_by_cluster_city.sql) | `data_marts.monthly_orders_by_cluster_and_city` | Количество заказов по ценовому сегменту (`low`/`middle`/`hight`) и городу магазина за месяц |
| **Data Marts** | Задание 5.3 | [`data_marts/03_yearly_avg_weekly_conversion.sql`](data_marts/03_yearly_avg_weekly_conversion.sql) | `data_marts.yearly_avg_weekly_conversion` | Средняя недельная конверсия интеракций на сайте в завершённые заказы с усреднением по году |
| **Data Marts** | Задание 5.4 | [`data_marts/04_user_product_orders_last_4m.sql`](data_marts/04_user_product_orders_last_4m.sql) | `data_marts.user_product_orders_last_4m` | Заказы пользователей по каждому товару за последние 4 месяца (широкая pivot-витрина собирается также в [`src/dwh/marts_builder.py`](../src/dwh/marts_builder.py)) |
| **Data Marts** | Задание 5.5 | [`data_marts/05_ml_predictions_vs_fact.sql`](data_marts/05_ml_predictions_vs_fact.sql) | `data_marts.ml_predictions_vs_fact_orders`<br>`data_marts.ml_predictions_vs_fact_revenue` | Сравнение ML-прогнозов с фактом по заказам (`completed`, `all`, `fact`) и выручке (`order_price + delivery_price - prime_cost`) |
