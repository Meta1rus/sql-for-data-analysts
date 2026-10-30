-- Задание 4.3: Таблица количества интеракций по типам за каждый день в локальном времени
-- (delivered считаем как completed)
CREATE DATABASE IF NOT EXISTS stage_layer;

CREATE TABLE IF NOT EXISTS stage_layer.daily_interactions_by_type (
    interaction_date Date32,
    interaction_type String,
    interactions_count UInt64
) ENGINE = MergeTree()
ORDER BY (interaction_date, interaction_type);

TRUNCATE TABLE stage_layer.daily_interactions_by_type;

INSERT INTO stage_layer.daily_interactions_by_type
WITH city_tz AS (
    SELECT
        city,
        any(timezone) AS timezone
    FROM raw_layer.shops
    GROUP BY city
),
all_events AS (
    SELECT
        i.user_id AS user_id,
        i.datetime AS event_dt_utc,
        if(i.interaction_type = 'delivered', 'completed', i.interaction_type) AS interaction_type
    FROM raw_layer.interactions AS i

    UNION ALL

    SELECT
        oh.user_id AS user_id,
        oh.datetime AS event_dt_utc,
        'completed' AS interaction_type
    FROM raw_layer.orders_history AS oh
    WHERE oh.stage IN ('completed', 'delivered')
)
SELECT
    toDate(
        toTimeZone(
            e.event_dt_utc,
             coalesce( nullIf(ct.timezone, ''), 'Europe/Moscow')
        )
    ) AS interaction_date,
    e.interaction_type AS interaction_type,
    count() AS interactions_count
FROM all_events AS e
LEFT JOIN raw_layer.users_cities AS uc ON e.user_id = uc.user_id
LEFT JOIN city_tz AS ct ON uc.city = ct.city
GROUP BY
    interaction_date,
    interaction_type;
