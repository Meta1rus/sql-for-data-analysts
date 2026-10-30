-- Задание 5.3: Средняя конверсия интеракций на сайте за неделю в завершённые заказы с усреднением по году
CREATE DATABASE IF NOT EXISTS data_marts;

CREATE TABLE IF NOT EXISTS data_marts.yearly_avg_weekly_conversion (
    report_year UInt16,
    weeks_count UInt32,
    total_site_interactions UInt64,
    total_completed_orders UInt64,
    avg_weekly_conversion Float64,
    avg_weekly_conversion_pct Float64
) ENGINE = MergeTree()
ORDER BY report_year;

TRUNCATE TABLE data_marts.yearly_avg_weekly_conversion;

INSERT INTO data_marts.yearly_avg_weekly_conversion
WITH weekly_stats AS (
    SELECT
        toYear(interaction_date) AS report_year,
        toStartOfWeek(interaction_date, 1) AS week_start,
        sumIf(
            interactions_count,
            interaction_type IN ('click', 'add_to_cart', 'purchased', 'paid')
        ) AS site_interactions,
        sumIf(
            interactions_count,
            interaction_type = 'completed'
        ) AS completed_orders,
        if(
            site_interactions > 0,
            toFloat64(completed_orders) / toFloat64(site_interactions),
            0.0
        ) AS weekly_conversion
    FROM stage_layer.daily_interactions_by_type
    GROUP BY
        report_year,
        week_start
)
SELECT
    report_year,
    count() AS weeks_count,
    sum(site_interactions) AS total_site_interactions,
    sum(completed_orders) AS total_completed_orders,
    round(avg(weekly_conversion), 6) AS avg_weekly_conversion,
    round(avg(weekly_conversion) * 100.0, 4) AS avg_weekly_conversion_pct
FROM weekly_stats
GROUP BY report_year
ORDER BY report_year;
