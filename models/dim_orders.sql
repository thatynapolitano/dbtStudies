{{
    config(
        materialized='table'
    )
}}

select * from `dbt-tutorial.jaffle_shop.orders`

