{{ config(materialized='table') }}

WITH min_max_dates AS (
    SELECT
        DATE_TRUNC('year', MIN(date)) AS min_date,  -- automatically get the first day of the earliest year
        MAX(date) AS max_date
    FROM {{ ref('stg_workout_journal') }}
),
date_series AS (
    SELECT gs.date::DATE AS date
    FROM generate_series(
        (SELECT min_date FROM min_max_dates),  -- dynamically generated start date
        (SELECT max_date FROM min_max_dates),
        INTERVAL '1 day'
    ) AS gs(date)
),
training_days AS (
    SELECT DISTINCT date AS date
    FROM {{ ref('stg_workout_journal') }}
),
training_and_rest_days AS (
    SELECT
        EXTRACT(YEAR FROM ds.date)::SMALLINT AS year,
        EXTRACT(MONTH FROM ds.date)::SMALLINT as month_int,
        LOWER(TO_CHAR(ds.date, 'Month')) AS month,
        ds.date,
        CASE 
            WHEN td.date IS NOT NULL THEN FALSE 
            ELSE TRUE 
        END AS rest
    FROM 
        date_series ds
    LEFT JOIN 
        training_days td ON ds.date = td.date
    ORDER BY
        date
)

SELECT * FROM training_and_rest_days
