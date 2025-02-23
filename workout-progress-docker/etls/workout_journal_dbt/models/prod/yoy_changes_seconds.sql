{{ config(materialized='table') }}

WITH yoy_changes_seconds AS (
    SELECT
        yc.year,
        yc.month_int,
        yc.month,
        yc.exercise,
        yc.last_year_reps AS last_year_seconds,
        yc.total_reps AS total_seconds,
        yc.added_weight,
        yc.weight_unit,
        yc.yoy_percentage_change,
        yc.yoy_absolute_change
    FROM
        {{ ref('yoy_changes') }} yc
    INNER JOIN
        {{ ref('exercises_calculated_in_seconds') }} ecis
    ON
        yc.exercise=ecis.exercise
    ORDER BY
        yc.year, yc.month_int, yc.exercise, yc.added_weight
)

SELECT * FROM yoy_changes_seconds