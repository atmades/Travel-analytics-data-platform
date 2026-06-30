{{ config(materialized='table') }}

SELECT
    ads.platform,
    ads.campaign_id,
    ads.campaign_name,
    ads.spend,
    ads.clicks,
    ads.impressions,
    countIf(orders.status IN ('paid', 'completed')) AS bookings,
    sumIf(orders.price, orders.status IN ('paid', 'completed')) AS revenue,
    if(
        countIf(orders.status IN ('paid', 'completed')) = 0,
        0,
        ads.spend / countIf(orders.status IN ('paid', 'completed'))
    ) AS cpa,
    if(
        ads.spend = 0,
        0,
        sumIf(orders.price, orders.status IN ('paid', 'completed')) / ads.spend
    ) AS roas
FROM {{ ref('stg_ads_latest') }} AS ads
LEFT JOIN {{ ref('stg_orders') }} AS orders
    ON ads.campaign_id = toString(orders.campaign_id)
GROUP BY
    ads.platform,
    ads.campaign_id,
    ads.campaign_name,
    ads.spend,
    ads.clicks,
    ads.impressions