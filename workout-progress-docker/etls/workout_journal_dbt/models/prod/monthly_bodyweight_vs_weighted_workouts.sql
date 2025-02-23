{{ config(materialized='table') }}

WITH monthly_number_bodyweight_vs_weighted_workouts AS (
    SELECT
        CAST(year AS SMALLINT),
        month_int,
        month,
        CAST(COUNT(CASE WHEN added_weight = 0.00 THEN 1 END) AS SMALLINT) AS bodyweight_sets,
        CAST(COUNT(CASE WHEN added_weight > 0.00 THEN 1 END) AS SMALLINT) AS weighted_sets
    FROM
        {{ ref('stg_workout_journal') }}
    GROUP BY
        year, month_int, month
),
monthly_percentage_bodyweight_vs_weighted_workout AS (
    SELECT 
        year,
        month_int,
        month,
        bodyweight_sets, 
        weighted_sets,
        ROUND(bodyweight_sets * 100.0 / NULLIF(bodyweight_sets + weighted_sets, 0), 2) AS bodyweight_percentage,
        ROUND(weighted_sets * 100.0 / NULLIF(bodyweight_sets + weighted_sets, 0), 2) AS weighted_percentage
    FROM 
        monthly_number_bodyweight_vs_weighted_workouts
    ORDER BY 
        year, month_int
)

SELECT * FROM monthly_percentage_bodyweight_vs_weighted_workout



