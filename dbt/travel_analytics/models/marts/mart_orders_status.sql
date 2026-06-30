{{ config(materialized='table') }}

SELECT
    status,
    count() AS orders_count,
    sum(price) AS total_revenue
FROM {{ ref('stg_orders') }}
GROUP BY status