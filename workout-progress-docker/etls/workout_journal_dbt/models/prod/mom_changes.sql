{{ config(materialized='table') }}

WITH mom_changes AS (
    SELECT
        year,
        month_int,
        month,
        exercise,
        CAST(
            LAG(total_reps) OVER (
                PARTITION BY exercise, added_weight
                ORDER BY year, month_int
            ) AS SMALLINT
        ) AS last_month_reps,
        CAST(total_reps AS SMALLINT),
        added_weight,
        weight_unit,
        -- handling MoM percentage change
        CASE 
            -- if last month had 0 reps and this month has reps, define it as 100% increase (or NULL/∞)
            WHEN LAG(total_reps) OVER (
                PARTITION BY exercise, added_weight 
                ORDER BY year, month_int
            ) = 0 AND total_reps > 0 
            THEN 100  -- or choose a value like 100, '∞', etc.
            
            -- standard percentage change calculation
            WHEN LAG(total_reps) OVER (
                PARTITION BY exercise, added_weight 
                ORDER BY year, month_int
            ) IS NOT NULL AND LAG(total_reps) OVER (
                PARTITION BY exercise, added_weight 
                ORDER BY year, month_int
            ) != 0 
            THEN 
                ROUND(
                    (CAST(total_reps AS NUMERIC) - CAST(LAG(total_reps) OVER (
                        PARTITION BY exercise, added_weight 
                        ORDER BY year, month_int
                    ) AS NUMERIC)) 
                    / CAST(LAG(total_reps) OVER (
                        PARTITION BY exercise, added_weight 
                        ORDER BY year, month_int
                    ) AS NUMERIC) * 100, 2
                )
            ELSE NULL
        END AS mom_percentage_change,
        -- absolute change in reps, since sometimes percentages can be misleading
        CAST(
            total_reps - COALESCE(LAG(total_reps) OVER (
                PARTITION BY exercise, added_weight 
                ORDER BY year, month_int
            ), 0) AS SMALLINT
        ) AS mom_absolute_change
    FROM 
        {{ ref('monthly_aggregates_every_possible_combination') }}
    ORDER BY 
        year, month_int, exercise, added_weight
)

SELECT * FROM mom_changes
