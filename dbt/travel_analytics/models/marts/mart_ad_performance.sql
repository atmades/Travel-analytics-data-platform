{{ config(materialized='table') }}

SELECT
    platform,
    campaign_id,
    campaign_name,
    clicks,
    impressions,
    spend,
    if(impressions = 0, 0, clicks / impressions) AS ctr,
    if(clicks = 0, 0, spend / clicks) AS cpc
FROM {{ ref('stg_ads_latest') }}