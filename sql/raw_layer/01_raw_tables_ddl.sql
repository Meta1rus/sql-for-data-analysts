-- DDL всех 13 таблиц слоя raw_layer в ClickHouse DWH

CREATE DATABASE IF NOT EXISTS raw_layer;

CREATE TABLE IF NOT EXISTS raw_layer.products (
    product_id Int64,
    category String,
    product_name String,
    prime_cost Int64
) ENGINE = MergeTree()
ORDER BY product_id;

CREATE TABLE IF NOT EXISTS raw_layer.shops (
    shop_id String,
    city String,
    timezone String,
    is_only_picking Bool,
    is_removed Bool,
    open_date Date32,
    closing_date Nullable(Date32)
) ENGINE = MergeTree()
ORDER BY shop_id;

CREATE TABLE IF NOT EXISTS raw_layer.users (
    user_id String,
    gender String,
    birthday Date32,
    registration_date DateTime64(6, 'UTC')
) ENGINE = MergeTree()
ORDER BY user_id;

CREATE TABLE IF NOT EXISTS raw_layer.interactions (
    interaction_id Int64,
    user_id String,
    product_id Int64,
    datetime DateTime64(6, 'UTC'),
    interaction_type String,
    order_id Nullable(String)
) ENGINE = MergeTree()
ORDER BY (datetime, interaction_id);

CREATE TABLE IF NOT EXISTS raw_layer.orders_history (
    user_id String,
    product_id Int64,
    datetime DateTime64(6, 'UTC'),
    order_id String,
    stage String,
    shop_id String
) ENGINE = MergeTree()
ORDER BY (order_id, datetime);

CREATE TABLE IF NOT EXISTS raw_layer.products_price_history (
    product_id Int64,
    effective_from DateTime64(6, 'UTC'),
    price Int64
) ENGINE = MergeTree()
ORDER BY (product_id, effective_from);

CREATE TABLE IF NOT EXISTS raw_layer.delivery_price_history (
    object_id String,
    object_type String,
    effective_from DateTime64(6, 'UTC'),
    price Int64
) ENGINE = MergeTree()
ORDER BY (object_type, object_id, effective_from);

CREATE TABLE IF NOT EXISTS raw_layer.competitors_products_price_history (
    parse_id String,
    product_id Int64,
    datetime DateTime64(6, 'UTC'),
    price Nullable(Float64),
    cost Nullable(Float64),
    value Nullable(Float64)
) ENGINE = MergeTree()
ORDER BY (product_id, datetime, parse_id);

CREATE TABLE IF NOT EXISTS raw_layer.shops_daily_completed_orders_predict (
    shop_id String,
    forecast_date DateTime64(6, 'UTC'),
    predict_orders Int64,
    creation_date Date32
) ENGINE = MergeTree()
ORDER BY (shop_id, forecast_date, creation_date);

CREATE TABLE IF NOT EXISTS raw_layer.shops_daily_all_orders_predict (
    shop_id String,
    forecast_date DateTime64(6, 'UTC'),
    predict_orders Int64,
    creation_date Date32
) ENGINE = MergeTree()
ORDER BY (shop_id, forecast_date, creation_date);

CREATE TABLE IF NOT EXISTS raw_layer.daily_revenue_predict (
    forecast_date DateTime64(6, 'UTC'),
    predict_revenue Int64,
    creation_date Date32,
    model_name String
) ENGINE = MergeTree()
ORDER BY (model_name, forecast_date, creation_date);

CREATE TABLE IF NOT EXISTS raw_layer.users_cities (
    user_id String,
    city String
) ENGINE = MergeTree()
ORDER BY user_id;

CREATE TABLE IF NOT EXISTS raw_layer.users_clusters (
    user_id String,
    price_cluster String
) ENGINE = MergeTree()
ORDER BY user_id;
