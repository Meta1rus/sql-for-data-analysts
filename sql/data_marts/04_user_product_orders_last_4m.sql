-- Задание 5.4: Витрина с текущим состоянием взаимодействий (заказов) пользователей
-- за последние 4 месяца с товаром (customer_id + колонки по ID каждого товара)
CREATE DATABASE IF NOT EXISTS data_marts;

CREATE TABLE IF NOT EXISTS data_marts.user_product_orders_last_4m (
    customer_id String,
    product_orders_map Map(Int64, UInt64),
    total_orders_last_4m UInt64
) ENGINE = MergeTree()
ORDER BY customer_id;

TRUNCATE TABLE data_marts.user_product_orders_last_4m;

INSERT INTO data_marts.user_product_orders_last_4m
WITH max_dt AS (
    SELECT max(toDate(created_at)) AS max_order_date
    FROM stage_layer.orders_full
),
agg_per_product AS (
    SELECT
        user_id AS customer_id,
        product_id,
        toUInt64(countDistinct(order_id)) AS orders_cnt
    FROM stage_layer.orders_full
    WHERE created_at IS NOT NULL
      AND toDate(created_at) >= subtractMonths((SELECT max_order_date FROM max_dt), 4)
    GROUP BY
        customer_id,
        product_id
)
SELECT
    customer_id,
    CAST((groupArray(product_id), groupArray(orders_cnt)), 'Map(Int64, UInt64)') AS product_orders_map,
    sum(orders_cnt) AS total_orders_last_4m
FROM agg_per_product
GROUP BY customer_id;
