{{ config(
    materialized='table'
) }}

SELECT
    status,
    count() AS total_bookings
FROM {{ ref('stg_bookings_latest') }}
GROUP BY status