{{ config(materialized='table') }}

SELECT
    route,
    transport_type,
    count() AS total_bookings,
    countIf(status = 'paid') AS paid_bookings,
    sumIf(price, status = 'paid') AS total_revenue
FROM {{ ref('stg_bookings_latest') }}
GROUP BY
    route,
    transport_type