{{ config(materialized='table') }}

WITH yearly_number_bodyweight_vs_weighted_workouts AS (
    SELECT
        CAST(year AS SMALLINT),
        CAST(COUNT(CASE WHEN added_weight = 0.00 THEN 1 END) AS SMALLINT) AS bodyweight_sets,
        CAST(COUNT(CASE WHEN added_weight > 0.00 THEN 1 END) AS SMALLINT) AS weighted_sets
    FROM
        {{ ref('stg_workout_journal') }}
    GROUP BY
        year
),
yearly_bodyweight_vs_weighted_workout AS (
    SELECT 
        year,
        bodyweight_sets, 
        weighted_sets,
        ROUND(bodyweight_sets * 100.0 / NULLIF(bodyweight_sets + weighted_sets, 0), 2) AS bodyweight_percentage,
        ROUND(weighted_sets * 100.0 / NULLIF(bodyweight_sets + weighted_sets, 0), 2) AS weighted_percentage
    FROM 
        yearly_number_bodyweight_vs_weighted_workouts
    ORDER BY 
        year
)

SELECT * FROM yearly_bodyweight_vs_weighted_workout



