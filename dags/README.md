# DAG'и оркестрации Apache Airflow

Airflow рекурсивно сканирует подпапки внутри `dags/`. Все 10 DAG'ов разбиты по трём слоям DWH и запускаются по расписанию `@daily`.

| Папка / файл | `dag_id` | Задание | Что делает |
|---|---|---|---|
| [`raw_layer/raw_layer_daily_dag.py`](raw_layer/raw_layer_daily_dag.py) | `raw_layer_daily_sync` | Задание 3 | Ежедневно синхронизирует 13 таблиц из 5 источников в `raw_layer` через `PySparkHandler` (`TRUNCATE` для справочников, `append` по watermark для таблиц событий и историй) |
| [`stage_layer/stage_competitors_price_history_dag.py`](stage_layer/stage_competitors_price_history_dag.py) | `stage_competitors_price_history` | Задание 4.1 | Пересобирает `stage_layer.competitors_price_history` |
| [`stage_layer/stage_ml_predictions_dag.py`](stage_layer/stage_ml_predictions_dag.py) | `stage_ml_predictions` | Задание 4.2 | Пересобирает витрины актуальных ML-прогнозов (`ml_completed_orders_predict`, `ml_all_orders_predict`, `ml_revenue_predict`) |
| [`stage_layer/stage_daily_interactions_dag.py`](stage_layer/stage_daily_interactions_dag.py) | `stage_daily_interactions_by_type` | Задание 4.3 | Пересобирает `stage_layer.daily_interactions_by_type` |
| [`stage_layer/stage_orders_full_dag.py`](stage_layer/stage_orders_full_dag.py) | `stage_orders_full` | Задание 4.4 | Пересобирает широкую таблицу заказов `stage_layer.orders_full` |
| [`data_marts/mart_daily_competitors_price_diff_dag.py`](data_marts/mart_daily_competitors_price_diff_dag.py) | `mart_daily_competitors_price_diff` | Задание 5.1 | Пересобирает `data_marts.daily_competitors_price_diff` |
| [`data_marts/mart_monthly_orders_by_cluster_city_dag.py`](data_marts/mart_monthly_orders_by_cluster_city_dag.py) | `mart_monthly_orders_by_cluster_and_city` | Задание 5.2 | Пересобирает `data_marts.monthly_orders_by_cluster_and_city` |
| [`data_marts/mart_yearly_avg_weekly_conversion_dag.py`](data_marts/mart_yearly_avg_weekly_conversion_dag.py) | `mart_yearly_avg_weekly_conversion` | Задание 5.3 | Пересобирает `data_marts.yearly_avg_weekly_conversion` |
| [`data_marts/mart_user_product_orders_last_4m_dag.py`](data_marts/mart_user_product_orders_last_4m_dag.py) | `mart_user_product_orders_last_4m` | Задание 5.4 | Пересобирает широкую pivot-витрину `data_marts.user_product_orders_last_4m` |
| [`data_marts/mart_ml_predictions_vs_fact_dag.py`](data_marts/mart_ml_predictions_vs_fact_dag.py) | `mart_ml_predictions_vs_fact` | Задание 5.5 | Пересобирает `data_marts.ml_predictions_vs_fact_orders` и `data_marts.ml_predictions_vs_fact_revenue` |
