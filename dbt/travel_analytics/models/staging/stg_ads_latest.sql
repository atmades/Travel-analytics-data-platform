{{ config(materialized='table') }}

SELECT
    platform,
    toString(campaign_id) AS campaign_id,
    campaign_name,
    clicks,
    impressions,
    spend,
    loaded_at
FROM
(
    SELECT
        *,
        row_number() OVER (
            PARTITION BY platform, campaign_id
            ORDER BY loaded_at DESC
        ) AS rn
    FROM
    (
        SELECT * FROM {{ source('raw', 'raw_google_ads') }}
        UNION ALL
        SELECT * FROM {{ source('raw', 'raw_meta_ads') }}
    )
)
WHERE rn = 1