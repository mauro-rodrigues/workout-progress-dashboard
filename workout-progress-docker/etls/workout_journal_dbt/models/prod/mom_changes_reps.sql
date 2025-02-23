{{ config(materialized='table') }}

WITH mom_changes_reps AS (
    SELECT
        mc.year,
        mc.month_int,
        mc.month,
        mc.exercise,
        mc.last_month_reps,
        mc.total_reps,
        mc.added_weight,
        mc.weight_unit,
        CAST(        
            CASE
                WHEN mc.mom_percentage_change IS NULL
                THEN -100.00
                ELSE mc.mom_percentage_change
            END AS NUMERIC(7,2)
        ) AS mom_percentage_change,
        mc.mom_absolute_change
    FROM
        {{ ref('mom_changes') }} mc
    INNER JOIN
        {{ ref('exercises_calculated_in_reps') }} ecir
    ON
        mc.exercise=ecir.exercise
    ORDER BY
        mc.year, mc.month_int, mc.exercise, mc.added_weight
)

SELECT * FROM mom_changes_reps