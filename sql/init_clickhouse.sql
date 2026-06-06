CREATE DATABASE IF NOT EXISTS travel;

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
)
ENGINE = MergeTree
ORDER BY (booking_id);