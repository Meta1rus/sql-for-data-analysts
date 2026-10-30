-- Задание 5.2: Количество заказов по кластерам ценового сегмента и городу назначения (магазина) за месяц
CREATE DATABASE IF NOT EXISTS data_marts;

CREATE TABLE IF NOT EXISTS data_marts.monthly_orders_by_cluster_and_city (
    report_month Date32,
    price_cluster String,
    shop_city String,
    orders_count UInt64,
    completed_orders_count UInt64
) ENGINE = MergeTree()
ORDER BY (report_month, price_cluster, shop_city);

TRUNCATE TABLE data_marts.monthly_orders_by_cluster_and_city;

INSERT INTO data_marts.monthly_orders_by_cluster_and_city
SELECT
    toStartOfMonth(toDate(created_at)) AS report_month,
    price_cluster,
    shop_city,
    countDistinct(order_id) AS orders_count,
    countDistinctIf(order_id, order_status = 'завершен') AS completed_orders_count
FROM stage_layer.orders_full
WHERE created_at IS NOT NULL
GROUP BY
    report_month,
    price_cluster,
    shop_city;
