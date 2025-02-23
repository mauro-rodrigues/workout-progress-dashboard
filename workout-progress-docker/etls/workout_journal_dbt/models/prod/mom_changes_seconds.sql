{{ config(materialized='table') }}

WITH mom_changes_seconds AS (
    SELECT
        mc.year,
        mc.month_int,
        mc.month,
        mc.exercise,
        mc.last_month_reps AS last_month_seconds,
        mc.total_reps AS total_seconds,
        mc.added_weight,
        mc.weight_unit,
        mc.mom_percentage_change,
        mc.mom_absolute_change
    FROM
        {{ ref('mom_changes') }} mc
    INNER JOIN
        {{ ref('exercises_calculated_in_seconds') }} ecis
    ON
        mc.exercise=ecis.exercise
    ORDER BY
        mc.year, mc.month_int, mc.exercise, mc.added_weight
)

SELECT * FROM mom_changes_seconds