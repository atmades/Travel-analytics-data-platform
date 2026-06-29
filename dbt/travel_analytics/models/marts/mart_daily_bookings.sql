{{ config(materialized='table') }}

SELECT
    toDate(created_at) AS booking_date,
    count() AS total_bookings,
    countIf(status = 'paid') AS paid_bookings,
    countIf(status = 'created') AS created_bookings,
    countIf(status = 'cancelled') AS cancelled_bookings,
    sumIf(price, status = 'paid') AS total_revenue
FROM {{ ref('stg_bookings_latest') }}
GROUP BY booking_date