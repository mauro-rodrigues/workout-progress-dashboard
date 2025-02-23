{{ config(materialized='table') }}

WITH min_max_dates AS (
    SELECT
        LEAST(DATE_TRUNC('month', '2021-01-01'::DATE), MIN(DATE_TRUNC('month', date))) AS min_month,
        MAX(DATE_TRUNC('month', date)) AS max_month
    FROM {{ ref('stg_workout_journal') }}
),
months_series AS (
    SELECT
        CAST(EXTRACT(YEAR FROM gs.date) AS SMALLINT) AS year,
        CAST(EXTRACT(MONTH FROM gs.date) AS SMALLINT) AS month_int,
        TRIM(LOWER(TO_CHAR(gs.date, 'Month'))) AS month
    FROM 
        generate_series(
            (SELECT min_month FROM min_max_dates),
            (SELECT max_month FROM min_max_dates),
            INTERVAL '1 month'
        ) AS gs(date)
),
exercise_combinations AS (
    SELECT DISTINCT exercise, added_weight
    FROM {{ ref('stg_workout_journal') }}
),
possible_year_month_exercise_combinations AS (
    SELECT 
        ms.year,
        ms.month_int,
        ms.month,
        ec.exercise,
        ec.added_weight
    FROM months_series ms
    CROSS JOIN exercise_combinations ec
),
existing_exercise_combinations AS (
    SELECT
        DISTINCT
            CAST(year AS SMALLINT),
            month_int,
            month,
            exercise,
            added_weight
    FROM {{ ref('stg_workout_journal') }}
),
year_month_exercise_combinations_with_no_data AS (
    SELECT
        pymec.year,
        pymec.month_int,
        pymec.month,
        pymec.exercise,
        pymec.added_weight
    FROM
        possible_year_month_exercise_combinations pymec
    LEFT JOIN
        existing_exercise_combinations eec
        ON pymec.year=eec.year
        AND pymec.month_int=eec.month_int
        AND pymec.exercise=eec.exercise
        AND pymec.added_weight=eec.added_weight 
    WHERE eec.year IS NULL -- keep only missing data
),
complete_combination_list AS (
    SELECT 
        * 
    FROM 
        existing_exercise_combinations
    UNION ALL
    SELECT 
        * 
    FROM 
        year_month_exercise_combinations_with_no_data
    ORDER BY 
        year, month_int, exercise, added_weight
),
monthly_aggregates_every_possible_combination AS (
    SELECT 
        ccl.year,
        ccl.month_int,
        ccl.month,
        ccl.exercise,
        COALESCE(mrpew.total_reps, 0) AS total_reps,
        ccl.added_weight,
        COALESCE(mrpew.weight_unit, 'kg') AS weight_unit
    FROM complete_combination_list ccl
    LEFT JOIN {{ ref('monthly_reps_per_exercise_weight') }} mrpew 
        ON ccl.year = mrpew.year
        AND ccl.month_int = mrpew.month_int
        AND ccl.exercise = mrpew.exercise
        AND ccl.added_weight = mrpew.added_weight
    LEFT JOIN {{ ref('monthly_seconds_per_exercise_weight') }} mspew 
        ON ccl.year = mspew.year
        AND ccl.month_int = mspew.month_int
        AND ccl.exercise = mspew.exercise
        AND ccl.added_weight = mspew.added_weight
    ORDER BY ccl.year, ccl.month_int, ccl.exercise, ccl.added_weight
)

SELECT * FROM monthly_aggregates_every_possible_combination