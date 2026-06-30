{{ config(materialized='table') }}

SELECT
    event_id,
    user_id,
    event_type,
    event_time,
    properties,
    source_topic,
    ingested_at
FROM
(
    SELECT
        *,
        row_number() OVER (
            PARTITION BY event_id
            ORDER BY ingested_at DESC
        ) AS rn
    FROM {{ source('raw', 'raw_user_events') }}
)
WHERE rn = 1