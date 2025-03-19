{{ config(materialized='view') }}

WITH tips_per_month AS (
    SELECT
        taxi_id,
        year_month,
        tips_sum
    FROM {{ ref('stg_tips_per_month') }}  -- Ссылаемся на модель stg_tips_per_month
)

SELECT taxi_id
FROM tips_per_month
WHERE year_month = '2018-04-01'  -- Фильтруем только апрель 2018 года
ORDER BY tips_sum DESC
LIMIT 3
