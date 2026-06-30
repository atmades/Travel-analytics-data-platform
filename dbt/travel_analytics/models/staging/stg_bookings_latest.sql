{{ config(
    materialized='table'
) }}

SELECT
    booking_id,
    user_id,
    route,
    transport_type,
    status,
    price,
    currency,
    created_at,
    loaded_at,
    run_id
FROM
(
    SELECT
        *,
        row_number() OVER (
            PARTITION BY booking_id
            ORDER BY loaded_at DESC
        ) AS rn
    FROM {{ source('raw', 'raw_bookings') }}
)
WHERE rn = 1