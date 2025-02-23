{{ config(materialized='table') }}

WITH last_year_reps_per_exercise_weight AS (
    SELECT
        CAST(year AS SMALLINT),
        CAST(month_int AS SMALLINT),
        month,
        exercise,
        CAST(
            CASE 
                WHEN LAG(month_int, 12) OVER (
                    PARTITION BY exercise, added_weight
                    ORDER BY year, month_int
                ) = month_int
                THEN LAG(total_reps, 12) OVER (
                    PARTITION BY exercise, added_weight
                    ORDER BY year, month_int
                )
                ELSE NULL
            END AS SMALLINT
        ) AS last_year_reps,
        CAST(total_reps AS SMALLINT),
        added_weight,
        weight_unit
    FROM
        {{ ref('monthly_aggregates_every_possible_combination') }}
),
yoy_changes AS (
    SELECT
        *,
        CASE
            WHEN last_year_reps = 0 AND total_reps > 0
            THEN 100
            WHEN last_year_reps > 0
            THEN 
                ROUND(
                    (CAST(total_reps AS NUMERIC) - CAST(LAG(total_reps, 12) OVER (
                        PARTITION BY exercise, added_weight
                        ORDER BY year, month_int
                    ) AS NUMERIC)) / 
                    NULLIF(CAST(LAG(total_reps, 12) OVER (
                        PARTITION BY exercise, added_weight
                        ORDER BY year, month_int
                    ) AS NUMERIC), 0) * 100, 2
                )
            ELSE NULL
        END AS yoy_percentage_change,
        CAST(
            total_reps - COALESCE(LAG(total_reps, 12) OVER (
                PARTITION BY exercise, added_weight 
                ORDER BY year, month_int
            ), 0) AS SMALLINT
        ) AS yoy_absolute_change
    FROM 
        last_year_reps_per_exercise_weight
    ORDER BY 
        year, month_int, exercise, added_weight
)

SELECT * FROM yoy_changes
