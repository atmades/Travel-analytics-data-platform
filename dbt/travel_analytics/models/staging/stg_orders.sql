{{ config(materialized='table') }}

SELECT
    order_id,
    user_id,
    route,
    transport_type,
    status,
    price,
    currency,
    campaign_id,
    created_at,
    updated_at,
    source_ts_ms
FROM
(
    SELECT
        JSONExtractUInt(after_record, 'order_id') AS order_id,
        JSONExtractUInt(after_record, 'user_id') AS user_id,
        JSONExtractString(after_record, 'route') AS route,
        JSONExtractString(after_record, 'transport_type') AS transport_type,
        JSONExtractString(after_record, 'status') AS status,
        JSONExtractFloat(after_record, 'price') AS price,
        JSONExtractString(after_record, 'currency') AS currency,
        JSONExtractUInt(after_record, 'campaign_id') AS campaign_id,
        parseDateTimeBestEffort(JSONExtractString(after_record, 'created_at')) AS created_at,
        parseDateTimeBestEffort(JSONExtractString(after_record, 'updated_at')) AS updated_at,
        source_ts_ms,
        row_number() OVER (
            PARTITION BY JSONExtractUInt(after_record, 'order_id')
            ORDER BY source_ts_ms DESC
        ) AS rn
    FROM {{ source('raw', 'raw_cdc_orders') }}
    WHERE op IN ('c', 'u', 'r')
      AND after_record != ''
)
WHERE rn = 1