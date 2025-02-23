{{ config(materialized='table') }}

WITH year_range AS (
    SELECT 
        MIN(year) AS min_year,
        MAX(year) AS max_year
    FROM 
		{{ ref('stg_workout_journal') }}
),
all_years AS (
    SELECT 
		generate_series(CAST(min_year AS INTEGER), CAST(max_year AS INTEGER)) AS year
    FROM 
		year_range
),
all_weekdays AS (
	SELECT
		DISTINCT weekday_int, weekday
	FROM
		{{ ref('stg_workout_journal') }}
),
year_weekday_combinations AS (
    SELECT 
        y.year,
        w.weekday_int,
        w.weekday
    FROM 
        all_years y
    CROSS JOIN 
        all_weekdays w
),
workout_frequency_weekday AS (
    SELECT
        CAST(year AS SMALLINT),
        weekday_int,
        weekday,
        CAST(COUNT(distinct workout_counter) AS SMALLINT) AS total_workouts
    FROM
        {{ ref('stg_workout_journal') }}
    GROUP BY
        year, weekday_int, weekday
),
workout_frequency_weekday_complete AS (
    SELECT 
        w.year,
        w.weekday_int,
        w.weekday,
        COALESCE(wf.total_workouts, 0) AS total_workouts
    FROM
        year_weekday_combinations w
    LEFT JOIN
        workout_frequency_weekday wf
        ON w.year = wf.year AND w.weekday_int = wf.weekday_int
    ORDER BY
        w.year, total_workouts DESC
)

SELECT * FROM workout_frequency_weekday_complete