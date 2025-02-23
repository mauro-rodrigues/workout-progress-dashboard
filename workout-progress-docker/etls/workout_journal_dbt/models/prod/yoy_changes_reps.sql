{{ config(materialized='table') }}

WITH yoy_changes_reps AS (
    SELECT
        yc.year,
        yc.month_int,
        yc.month,
        yc.exercise,
        yc.last_year_reps,
        yc.total_reps,
        yc.added_weight,
        yc.weight_unit,
        yc.yoy_percentage_change,
        yc.yoy_absolute_change
    FROM
        {{ ref('yoy_changes') }} yc
    INNER JOIN
        {{ ref('exercises_calculated_in_reps') }} ecir
    ON
        yc.exercise=ecir.exercise
    ORDER BY
        yc.year, yc.month_int, yc.exercise, yc.added_weight
)

SELECT * FROM yoy_changes_reps