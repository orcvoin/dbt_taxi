{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree()',
    order_by='(taxi_id, year_month)'
) }}

WITH tips_per_month AS (
    SELECT
        taxi_id,
        year_month,
        tips_sum
    FROM {{ ref('stg_tips_per_month') }}  -- Ссылаемся на модель stg_tips_per_month
),

top_taxis AS (
    SELECT taxi_id
    FROM {{ ref('stg_top_taxis_april_2018') }}  -- Ссылаемся на модель stg_top_taxis_april_2018
),

tips_with_lag AS (
    SELECT
        taxi_id,
        year_month,
        tips_sum,
        any(tips_sum) OVER (PARTITION BY taxi_id ORDER BY year_month ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS prev_tips_sum
    FROM tips_per_month
)
SELECT
    taxi_id,
    year_month,
    tips_sum,
    if(prev_tips_sum = 0, 0, round((tips_sum - prev_tips_sum) / prev_tips_sum * 100, 2)) AS tips_change
FROM tips_with_lag
where year_month >= '2018-04-01' and taxi_id in top_taxis
ORDER BY taxi_id, year_month
