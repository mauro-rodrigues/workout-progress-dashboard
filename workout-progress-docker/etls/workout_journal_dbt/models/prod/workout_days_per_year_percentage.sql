{{ config(materialized='table') }}

WITH training_days AS (
    SELECT 
        DISTINCT 
            date, 
            year
    FROM 
        {{ ref('workout_vs_rest_days') }}
    WHERE
        NOT rest
),
workout_sessions_per_year AS (
    SELECT 
        CAST(year AS SMALLINT),
        CAST(COUNT(*) AS SMALLINT) AS workout_days
    FROM 
        training_days
    GROUP BY 
        year
),
year_days AS (
    SELECT 
        DISTINCT 
            year,
            CAST(
                CASE 
                    WHEN (year::SMALLINT % 4 = 0 AND year::SMALLINT % 100 <> 0)
                    OR (year::SMALLINT % 400 = 0)
                    THEN 366 ELSE 365 
                END AS SMALLINT
            ) AS total_days
    FROM 
        training_days
),
workout_days_per_year_percentage AS (
    SELECT 
        wspy.year,
        yd.total_days,
        wspy.workout_days,
        ROUND((wspy.workout_days::DECIMAL / yd.total_days) * 100, 2) AS workout_percentage,
        ROUND(100 - (wspy.workout_days::DECIMAL / yd.total_days) * 100, 2) AS rest_percentage
    FROM 
        workout_sessions_per_year wspy
    JOIN 
        year_days yd ON wspy.year = yd.year
    ORDER BY 
        year
)

SELECT * FROM workout_days_per_year_percentage
