{{ config(materialized='table') }}

SELECT
    route,
    count() AS orders_count,
    sum(price) AS total_revenue,
    avg(price) AS avg_order_value
FROM {{ ref('stg_orders') }}
WHERE status IN ('paid', 'completed')
GROUP BY route