
{{
    config(
        materialized='table'
    )
}}
select * from `dbt-tutorial.stripe.payment`