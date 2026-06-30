{{ config(materialized='table') }}

WITH
    searches AS (
        SELECT count() AS cnt
        FROM {{ ref('stg_user_events') }}
        WHERE event_type = 'search_performed'
    ),

    bookings AS (
        SELECT count() AS cnt
        FROM {{ ref('stg_user_events') }}
        WHERE event_type = 'booking_started'
    ),

    payments AS (
        SELECT count() AS cnt
        FROM {{ ref('stg_user_events') }}
        WHERE event_type = 'payment_completed'
    )

SELECT
    'searches' AS metric,
    toFloat64((SELECT cnt FROM searches)) AS value

UNION ALL

SELECT
    'bookings' AS metric,
    toFloat64((SELECT cnt FROM bookings)) AS value

UNION ALL

SELECT
    'payments' AS metric,
    toFloat64((SELECT cnt FROM payments)) AS value

UNION ALL

SELECT
    'search_to_booking_rate' AS metric,
    if(
        (SELECT cnt FROM searches) = 0,
        0,
        (SELECT cnt FROM bookings) / (SELECT cnt FROM searches)
    ) AS value

UNION ALL

SELECT
    'booking_to_payment_rate' AS metric,
    if(
        (SELECT cnt FROM bookings) = 0,
        0,
        (SELECT cnt FROM payments) / (SELECT cnt FROM bookings)
    ) AS value