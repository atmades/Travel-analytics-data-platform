{{ config(materialized='table') }}

SELECT
    event_type,
    uniqExact(user_id) AS users_count,
    count() AS events_count
FROM {{ ref('stg_user_events') }}
GROUP BY event_type