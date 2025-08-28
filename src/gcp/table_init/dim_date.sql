CREATE OR REPLACE TABLE `thesis-25-456305.mart_layer.dim_date` AS
WITH dim_date_generate AS (
    SELECT date
    FROM UNNEST(GENERATE_DATE_ARRAY('2025-01-01', '2026-01-01', INTERVAL 1 DAY)) AS date
),
dim_date_enrich AS (
    SELECT
        date AS date_key,
        FORMAT_DATE('%F', date) AS full_date,
        FORMAT_DATE('%A', date) AS day_of_week,
        FORMAT_DATE('%a', date) AS day_of_week_short,
        FORMAT_DATE('%Y-%m', date) AS year_month,
        FORMAT_DATE('%B', date) AS month,
        FORMAT_DATE('%Y', date) AS year,
        EXTRACT(YEAR FROM date) AS year_number
    FROM dim_date_generate
)
SELECT
    *,
    CASE
        WHEN day_of_week_short IN ('Mon', 'Tue', 'Wed', 'Thu', 'Fri') THEN 'Weekday'
        WHEN day_of_week_short IN ('Sat', 'Sun') THEN 'Weekend'
        ELSE 'Invalid'
    END AS is_weekday_or_weekend
FROM dim_date_enrich;
