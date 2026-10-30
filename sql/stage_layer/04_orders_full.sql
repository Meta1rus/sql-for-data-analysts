-- Задание 4.4: Полная таблица заказов в stage_layer со всеми стадиями в локальном времени и ASOF JOIN цен
CREATE DATABASE IF NOT EXISTS stage_layer;

CREATE TABLE IF NOT EXISTS stage_layer.orders_full (
    user_id String,
    product_id Int64,
    order_id String,
    shop_id String,
    created_at Nullable(DateTime64(6)),
    accepted_at Nullable(DateTime64(6)),
    collecting_order_at Nullable(DateTime64(6)),
    sent_to_shop_at Nullable(DateTime64(6)),
    ready_at Nullable(DateTime64(6)),
    waiting_pickup_at Nullable(DateTime64(6)),
    sent_via_delivery_at Nullable(DateTime64(6)),
    completed_at Nullable(DateTime64(6)),
    delivered_at Nullable(DateTime64(6)),
    canceled_at Nullable(DateTime64(6)),
    lost_at Nullable(DateTime64(6)),
    order_status String,
    order_price Int64,
    delivery_price Int64,
    price_cluster String,
    user_city String,
    shop_city String,
    prime_cost Int64,
    last_known_stage String
) ENGINE = MergeTree()
ORDER BY (order_id, user_id, product_id);

TRUNCATE TABLE stage_layer.orders_full;

INSERT INTO stage_layer.orders_full
WITH orders_pivoted AS (
    SELECT
        order_id,
        any(user_id) AS user_id,
        any(product_id) AS product_id,
        any(shop_id) AS shop_id,
        min(datetime) AS order_start_utc,
        minIfOrNull(datetime, stage = 'created') AS created_utc,
        minIfOrNull(datetime, stage = 'accepted') AS accepted_utc,
        minIfOrNull(datetime, stage = 'collecting_order') AS collecting_order_utc,
        minIfOrNull(datetime, stage = 'sent_to_shop') AS sent_to_shop_utc,
        minIfOrNull(datetime, stage = 'ready') AS ready_utc,
        minIfOrNull(datetime, stage = 'waiting_pickup') AS waiting_pickup_utc,
        minIfOrNull(datetime, stage = 'sent_via_delivery') AS sent_via_delivery_utc,
        minIfOrNull(datetime, stage = 'completed') AS completed_utc,
        minIfOrNull(datetime, stage = 'delivered') AS delivered_utc,
        minIfOrNull(datetime, stage = 'canceled') AS canceled_utc,
        minIfOrNull(datetime, stage = 'lost') AS lost_utc,
        argMax(stage, datetime) AS last_known_stage
    FROM raw_layer.orders_history
    GROUP BY order_id
),
shop_delivery_prices AS (
    SELECT
        object_id AS shop_id,
        effective_from,
        price AS shop_delivery_price
    FROM raw_layer.delivery_price_history
    WHERE object_type = 'shop'
),
city_delivery_prices AS (
    SELECT
        object_id AS city_name,
        effective_from,
        price AS city_delivery_price
    FROM raw_layer.delivery_price_history
    WHERE object_type = 'city'
)
SELECT
    o.user_id AS user_id,
    o.product_id AS product_id,
    o.order_id AS order_id,
    o.shop_id AS shop_id,
    toTimeZone(o.created_utc, coalesce(nullIf(s.timezone, ''), 'Europe/Moscow')) AS created_at,
    toTimeZone(o.accepted_utc, coalesce(nullIf(s.timezone, ''), 'Europe/Moscow')) AS accepted_at,
    toTimeZone(o.collecting_order_utc, coalesce(nullIf(s.timezone, ''), 'Europe/Moscow')) AS collecting_order_at,
    toTimeZone(o.sent_to_shop_utc, coalesce(nullIf(s.timezone, ''), 'Europe/Moscow')) AS sent_to_shop_at,
    toTimeZone(o.ready_utc, coalesce(nullIf(s.timezone, ''), 'Europe/Moscow')) AS ready_at,
    toTimeZone(o.waiting_pickup_utc, coalesce(nullIf(s.timezone, ''), 'Europe/Moscow')) AS waiting_pickup_at,
    toTimeZone(o.sent_via_delivery_utc, coalesce(nullIf(s.timezone, ''), 'Europe/Moscow')) AS sent_via_delivery_at,
    toTimeZone(o.completed_utc, coalesce(nullIf(s.timezone, ''), 'Europe/Moscow')) AS completed_at,
    toTimeZone(o.delivered_utc, coalesce(nullIf(s.timezone, ''), 'Europe/Moscow')) AS delivered_at,
    toTimeZone(o.canceled_utc, coalesce(nullIf(s.timezone, ''), 'Europe/Moscow')) AS canceled_at,
    toTimeZone(o.lost_utc, coalesce(nullIf(s.timezone, ''), 'Europe/Moscow')) AS lost_at,
    multiIf(
        o.last_known_stage IN ('completed', 'delivered'), 'завершен',
        o.last_known_stage IN ('canceled', 'lost'), 'отменен/потерян',
        'в процессе'
    ) AS order_status,
    coalesce(pph.price, p.prime_cost) AS order_price,
    coalesce(nullIf(sdp.shop_delivery_price, 0), cdp.city_delivery_price, 0) AS delivery_price,
    coalesce(ucl.price_cluster, 'middle') AS price_cluster,
    coalesce(uci.city, '') AS user_city,
    coalesce(s.city, '') AS shop_city,
    coalesce(p.prime_cost, 0) AS prime_cost,
    o.last_known_stage AS last_known_stage
FROM orders_pivoted AS o
LEFT JOIN raw_layer.shops AS s
    ON o.shop_id = s.shop_id
LEFT JOIN raw_layer.products AS p
    ON o.product_id = p.product_id
LEFT JOIN raw_layer.users_clusters AS ucl
    ON o.user_id = ucl.user_id
LEFT JOIN raw_layer.users_cities AS uci
    ON o.user_id = uci.user_id
ASOF LEFT JOIN raw_layer.products_price_history AS pph
    ON o.product_id = pph.product_id AND o.order_start_utc >= pph.effective_from
ASOF LEFT JOIN shop_delivery_prices AS sdp
    ON o.shop_id = sdp.shop_id AND o.order_start_utc >= sdp.effective_from
ASOF LEFT JOIN city_delivery_prices AS cdp
    ON s.city = cdp.city_name AND o.order_start_utc >= cdp.effective_from;
