-- Задание 4.2: Витрины с актуальными (последними созданными) ML-прогнозами в stage_layer
CREATE DATABASE IF NOT EXISTS stage_layer;

-- 1. Прогноз завершённых заказов по магазинам (последний созданный прогноз)
CREATE TABLE IF NOT EXISTS stage_layer.ml_completed_orders_predict (
    shop_id String,
    forecast_date Date32,
    predict_orders Int64,
    creation_date Date32
) ENGINE = MergeTree()
ORDER BY (shop_id, forecast_date);

TRUNCATE TABLE stage_layer.ml_completed_orders_predict;

INSERT INTO stage_layer.ml_completed_orders_predict
SELECT
    shop_id,
    toDate(forecast_date) AS forecast_date,
    argMax(predict_orders, creation_date) AS predict_orders,
    max(creation_date) AS creation_date
FROM raw_layer.shops_daily_completed_orders_predict
GROUP BY
    shop_id,
    toDate(forecast_date);

-- 2. Прогноз всех заказов по магазинам (последний созданный прогноз)
CREATE TABLE IF NOT EXISTS stage_layer.ml_all_orders_predict (
    shop_id String,
    forecast_date Date32,
    predict_orders Int64,
    creation_date Date32
) ENGINE = MergeTree()
ORDER BY (shop_id, forecast_date);

TRUNCATE TABLE stage_layer.ml_all_orders_predict;

INSERT INTO stage_layer.ml_all_orders_predict
SELECT
    shop_id,
    toDate(forecast_date) AS forecast_date,
    argMax(predict_orders, creation_date) AS predict_orders,
    max(creation_date) AS creation_date
FROM raw_layer.shops_daily_all_orders_predict
GROUP BY
    shop_id,
    toDate(forecast_date);

-- 3. Прогноз выручки для каждой модели (catboost и TRex — последний созданный прогноз)
CREATE TABLE IF NOT EXISTS stage_layer.ml_revenue_predict (
    forecast_date Date32,
    model_name String,
    predict_revenue Int64,
    creation_date Date32
) ENGINE = MergeTree()
ORDER BY (model_name, forecast_date);

TRUNCATE TABLE stage_layer.ml_revenue_predict;

INSERT INTO stage_layer.ml_revenue_predict
SELECT
    toDate(forecast_date) AS forecast_date,
    model_name,
    argMax(predict_revenue, creation_date) AS predict_revenue,
    max(creation_date) AS creation_date
FROM raw_layer.daily_revenue_predict
GROUP BY
    toDate(forecast_date),
    model_name;
