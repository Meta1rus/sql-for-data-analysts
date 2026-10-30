-- Задание 5.1: Средняя разница между ценой на товар у нашей компании и у компаний-конкурентов за день
CREATE DATABASE IF NOT EXISTS data_marts;

CREATE TABLE IF NOT EXISTS data_marts.daily_competitors_price_diff (
    report_date Date32,
    product_id Int64,
    product_name String,
    avg_our_price Float64,
    avg_competitor_price Float64,
    avg_price_diff Float64
) ENGINE = MergeTree()
ORDER BY (report_date, product_id);

TRUNCATE TABLE data_marts.daily_competitors_price_diff;

INSERT INTO data_marts.daily_competitors_price_diff
WITH our_prices AS (
    SELECT
        product_id,
        toTimeZone(effective_from, 'Europe/Moscow') AS effective_from_local,
        toFloat64(price) AS our_price
    FROM raw_layer.products_price_history
)
SELECT
    c.parse_date AS report_date,
    c.product_id AS product_id,
    any(coalesce(p.product_name, '')) AS product_name,
    round(avg(coalesce(op.our_price, toFloat64(p.prime_cost))), 2) AS avg_our_price,
    round(avg(c.competitor_price), 2) AS avg_competitor_price,
    round(
        avg(coalesce(op.our_price, toFloat64(p.prime_cost)) - c.competitor_price),
        2
    ) AS avg_price_diff
FROM stage_layer.competitors_price_history AS c
LEFT JOIN raw_layer.products AS p
    ON c.product_id = p.product_id
ASOF LEFT JOIN our_prices AS op
    ON c.product_id = op.product_id AND c.parse_datetime >= op.effective_from_local
GROUP BY
    c.parse_date,
    c.product_id;
