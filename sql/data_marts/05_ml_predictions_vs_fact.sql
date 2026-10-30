-- Задание 5.5: Сравнение ML-прогнозов с действительностью (заказы и выручка) в локальном времени
CREATE DATABASE IF NOT EXISTS data_marts;

-- 1. Сравнение прогнозов заказов (completed, all) с фактом (fact_completed, fact_all) по магазинам и дням
CREATE TABLE IF NOT EXISTS data_marts.ml_predictions_vs_fact_orders (
    report_date Date32,
    shop_id String,
    shop_city String,
    predict_completed_orders Int64,
    predict_all_orders Int64,
    fact_completed_orders UInt64,
    fact_all_orders UInt64,
    completed_orders_diff Int64,
    all_orders_diff Int64
) ENGINE = MergeTree()
ORDER BY (report_date, shop_id);

TRUNCATE TABLE data_marts.ml_predictions_vs_fact_orders;

INSERT INTO data_marts.ml_predictions_vs_fact_orders
WITH fact_orders AS (
    SELECT
        toDate(created_at) AS report_date,
        shop_id,
        any(shop_city) AS shop_city,
        countDistinct(order_id) AS fact_all_orders,
        countDistinctIf(order_id, order_status = 'завершен') AS fact_completed_orders
    FROM stage_layer.orders_full
    WHERE created_at IS NOT NULL
    GROUP BY
        report_date,
        shop_id
)
SELECT
    coalesce(comp.forecast_date, all_p.forecast_date, f.report_date) AS report_date,
    coalesce(comp.shop_id, all_p.shop_id, f.shop_id) AS shop_id,
    coalesce(nullIf(f.shop_city, ''), s.city, '') AS shop_city,
    coalesce(comp.predict_orders, 0) AS predict_completed_orders,
    coalesce(all_p.predict_orders, 0) AS predict_all_orders,
    coalesce(f.fact_completed_orders, 0) AS fact_completed_orders,
    coalesce(f.fact_all_orders, 0) AS fact_all_orders,
    toInt64(coalesce(f.fact_completed_orders, 0)) - coalesce(comp.predict_orders, 0) AS completed_orders_diff,
    toInt64(coalesce(f.fact_all_orders, 0)) - coalesce(all_p.predict_orders, 0) AS all_orders_diff
FROM stage_layer.ml_completed_orders_predict AS comp
FULL OUTER JOIN stage_layer.ml_all_orders_predict AS all_p
    ON comp.shop_id = all_p.shop_id AND comp.forecast_date = all_p.forecast_date
FULL OUTER JOIN fact_orders AS f
    ON coalesce(comp.shop_id, all_p.shop_id) = f.shop_id
   AND coalesce(comp.forecast_date, all_p.forecast_date) = f.report_date
LEFT JOIN raw_layer.shops AS s
    ON coalesce(comp.shop_id, all_p.shop_id, f.shop_id) = s.shop_id;

-- 2. Сравнение прогнозов дохода (predicted_revenue по моделям catboost и TRex) с фактическим доходом (fact_revenue)
-- Доход = цена заказа + цена доставки - себестоимость товара
CREATE TABLE IF NOT EXISTS data_marts.ml_predictions_vs_fact_revenue (
    report_date Date32,
    model_name String,
    predicted_revenue Int64,
    fact_revenue Int64,
    revenue_diff Int64,
    relative_error_pct Float64
) ENGINE = MergeTree()
ORDER BY (report_date, model_name);

TRUNCATE TABLE data_marts.ml_predictions_vs_fact_revenue;

INSERT INTO data_marts.ml_predictions_vs_fact_revenue
WITH daily_fact_revenue AS (
    SELECT
        toDate(coalesce(completed_at, delivered_at, created_at)) AS report_date,
        sum(toInt64(order_price) + toInt64(delivery_price) - toInt64(prime_cost)) AS fact_revenue
    FROM stage_layer.orders_full
    WHERE order_status = 'завершен'
      AND coalesce(completed_at, delivered_at, created_at) IS NOT NULL
    GROUP BY report_date
)
SELECT
    p.forecast_date AS report_date,
    p.model_name AS model_name,
    p.predict_revenue AS predicted_revenue,
    coalesce(f.fact_revenue, 0) AS fact_revenue,
    coalesce(f.fact_revenue, 0) - p.predict_revenue AS revenue_diff,
    round(
        if(
            p.predict_revenue != 0,
            abs(toFloat64(coalesce(f.fact_revenue, 0) - p.predict_revenue))
            / toFloat64(p.predict_revenue)
            * 100.0,
            0.0
        ),
        2
    ) AS relative_error_pct
FROM stage_layer.ml_revenue_predict AS p
LEFT JOIN daily_fact_revenue AS f
    ON p.forecast_date = f.report_date;
