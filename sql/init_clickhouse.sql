CREATE DATABASE IF NOT EXISTS travel;

-- Raw layer
CREATE TABLE IF NOT EXISTS travel.raw_bookings
(
    booking_id UInt64,
    user_id UInt64,
    route String,
    transport_type String,
    status String,
    price Float64,
    currency String,
    created_at DateTime64(3, 'UTC'),
    loaded_at DateTime64(3, 'UTC') DEFAULT now64(3)
    run_id String
)
ENGINE = MergeTree
ORDER BY (booking_id, loaded_at);

CREATE TABLE IF NOT EXISTS travel.raw_user_events
(
    event_id String,
    user_id UInt64,
    event_type String,
    event_time DateTime64(3, 'UTC'),
    properties String,
    source_topic String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (event_id, ingested_at);

CREATE TABLE IF NOT EXISTS travel.raw_cdc_orders
(
    order_id Nullable(UInt64),
    op String,
    before_record String,
    after_record String,
    source_topic String,
    source_ts_ms Nullable(UInt64),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (order_id, source_ts_ms, ingested_at);

CREATE TABLE IF NOT EXISTS travel.raw_ads_api_payloads
(
    source String,
    endpoint String,
    payload String,
    fetched_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (source, endpoint, fetched_at);

CREATE TABLE IF NOT EXISTS travel.raw_google_ads
(
    campaign_id UInt64,
    campaign_name String,
    clicks UInt64,
    impressions UInt64,
    spend Float64,
    platform String,
    loaded_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (campaign_id, loaded_at);

CREATE TABLE IF NOT EXISTS travel.raw_meta_ads
(
    campaign_id UInt64,
    campaign_name String,
    clicks UInt64,
    impressions UInt64,
    spend Float64,
    platform String,
    loaded_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (campaign_id, loaded_at);

-- DLQ layer
CREATE TABLE IF NOT EXISTS travel.dlq_bookings
(
    booking_id UInt64,
    raw_record String,
    error_reason String,
    failed_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (booking_id, failed_at);

CREATE TABLE IF NOT EXISTS travel.dlq_user_events
(
    event_id Nullable(String),
    raw_event String,
    error_reason String,
    source_topic String,
    failed_at DateTime64(3, 'UTC') DEFAULT now64(3),
    recovered UInt8 DEFAULT 0,
    recovered_at Nullable(DateTime64(3, 'UTC')),
    replay_count UInt8 DEFAULT 0,
    replayed UInt8 DEFAULT 0,
    replayed_at Nullable(DateTime64(3, 'UTC'))
)
ENGINE = MergeTree
ORDER BY (failed_at);

-- Staging layer
CREATE TABLE IF NOT EXISTS travel.stg_bookings
(
    booking_id UInt64,
    user_id UInt64,
    route String,
    transport_type String,
    status String,
    price Float64,
    currency String,
    created_at DateTime64(3, 'UTC'),
    loaded_at DateTime64(3, 'UTC')
    run_id String
)
ENGINE = MergeTree
ORDER BY (booking_id);


CREATE TABLE IF NOT EXISTS travel.stg_bookings_latest
(
    booking_id UInt64,
    user_id UInt64,
    route String,
    transport_type String,
    status String,
    price Float64,
    currency String,
    created_at DateTime,
    loaded_at DateTime64(3, 'UTC'),
    run_id String
)
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY booking_id;


CREATE TABLE IF NOT EXISTS travel.stg_ads
(
    platform String,
    campaign_id UInt64,
    campaign_name String,
    clicks UInt64,
    impressions UInt64,
    spend Float64,
    loaded_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (platform, campaign_id);


CREATE TABLE IF NOT EXISTS travel.stg_ads_latest
(
    platform String,
    campaign_id String,
    campaign_name String,
    clicks UInt64,
    impressions UInt64,
    spend Float64,
    loaded_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (platform, campaign_id);


CREATE TABLE IF NOT EXISTS travel.stg_orders
(
    order_id UInt64,
    user_id UInt64,
    route String,
    transport_type String,
    status String,
    price Float64,
    currency String,
    campaign_id UInt64,
    created_at DateTime,
    updated_at DateTime,
    source_ts_ms UInt64
)
ENGINE = MergeTree
ORDER BY (order_id);

CREATE TABLE IF NOT EXISTS travel.stg_user_events
(
    event_id String,
    user_id UInt64,
    event_type String,
    event_time DateTime64(3, 'UTC'),
    properties String,
    source_topic String,
    ingested_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (event_id);

-- Data quality layer
CREATE TABLE IF NOT EXISTS travel.dq_results
(
    check_name String,
    check_status String,
    check_value Float64,
    check_threshold Float64,
    checked_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (check_name, checked_at);

-- Marts layer
CREATE TABLE IF NOT EXISTS travel.mart_daily_bookings
(
    booking_date Date,
    total_bookings UInt64,
    paid_bookings UInt64,
    created_bookings UInt64,
    cancelled_bookings UInt64,
    total_revenue Float64
)
ENGINE = MergeTree
ORDER BY (booking_date);

CREATE TABLE IF NOT EXISTS travel.mart_route_revenue
(
    route String,
    transport_type String,
    total_bookings UInt64,
    paid_bookings UInt64,
    total_revenue Float64
)
ENGINE = MergeTree
ORDER BY (route, transport_type);

CREATE TABLE IF NOT EXISTS travel.mart_booking_status
(
    status String,
    total_bookings UInt64
)
ENGINE = MergeTree
ORDER BY (status);

CREATE TABLE IF NOT EXISTS travel.mart_ad_performance
(
    platform String,
    campaign_id UInt64,
    campaign_name String,
    clicks UInt64,
    impressions UInt64,
    spend Float64,
    ctr Float64,
    cpc Float64
)
ENGINE = MergeTree
ORDER BY (platform, campaign_id);

CREATE TABLE IF NOT EXISTS travel.mart_campaign_performance
(
    platform String,
    campaign_id UInt64,
    campaign_name String,
    spend Float64,
    clicks UInt64,
    impressions UInt64,
    bookings UInt64,
    revenue Float64,
    cpa Float64,
    roas Float64
)
ENGINE = MergeTree
ORDER BY (platform, campaign_id);

CREATE TABLE IF NOT EXISTS travel.mart_orders_status
(
    status String,
    orders_count UInt64,
    total_revenue Float64
)
ENGINE = MergeTree
ORDER BY (status);

CREATE TABLE IF NOT EXISTS travel.mart_route_revenue_from_orders
(
    route String,
    orders_count UInt64,
    total_revenue Float64,
    avg_order_value Float64
)
ENGINE = MergeTree
ORDER BY (route);

CREATE TABLE IF NOT EXISTS travel.mart_user_event_funnel
(
    event_type String,
    users_count UInt64,
    events_count UInt64
)
ENGINE = MergeTree
ORDER BY (event_type);

CREATE TABLE IF NOT EXISTS travel.mart_booking_conversion
(
    metric String,
    value Float64
)
ENGINE = MergeTree
ORDER BY (metric);

CREATE TABLE IF NOT EXISTS travel.mart_dq_latest_results
(
    check_name String,
    check_status String,
    check_value Float64,
    check_threshold Float64,
    checked_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (check_name);
