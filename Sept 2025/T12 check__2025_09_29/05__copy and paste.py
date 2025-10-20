# Databricks notebook source


# COMMAND ----------

create_a_sequence_array as (
    select
        sequence(0, CAST(months_between(
            least(
            add_months(max_day, 12),
            date_trunc('month', MAX(date) over ())
            )
            , 
            greatest(
            add_months(min_day, -12),
            date('2023-01-01')
            )
            ) AS INT) ) as month_sequence,
        *
    from calculate_date_range
    ),
