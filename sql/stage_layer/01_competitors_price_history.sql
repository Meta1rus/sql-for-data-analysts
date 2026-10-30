-- Задание 4.1: Витрина с очищенной историей цены на товар у конкурентов в stage_layer
CREATE DATABASE IF NOT EXISTS stage_layer;

CREATE TABLE IF NOT EXISTS stage_layer.competitors_price_history (
    parse_id String,
    product_id Int64,
    company_name String,
    parse_datetime DateTime64(6, 'Europe/Moscow'),
    parse_date Date32,
    competitor_price Float64
) ENGINE = MergeTree()
ORDER BY (product_id, company_name, parse_date, parse_id);

TRUNCATE TABLE stage_layer.competitors_price_history;

INSERT INTO stage_layer.competitors_price_history
SELECT
    parse_id,
    product_id,
    company_name,
    toTimeZone(datetime, 'Europe/Moscow') AS parse_datetime,
    toDate(toTimeZone(datetime, 'Europe/Moscow')) AS parse_date,
    coalesce(price, cost, value) AS competitor_price
FROM raw_layer.competitors_products_price_history
WHERE coalesce(price, cost, value) IS NOT NULL;
