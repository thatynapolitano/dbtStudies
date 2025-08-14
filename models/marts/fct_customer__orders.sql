-- Import CTEs
with base_customers as (
    select * from {{ ref('stg_jaffle_shop_customers') }}
),

base_orders as (     
    select * from {{ ref('stg_jaffle_shop_orders') }}
),

payments as (
    select * from {{ ref('stg_stripe_payment') }}
),

-- Staging  
customer_names as (
    select
        id as customer_id,
        last_name as surname,
        first_name as givenname,
        first_name || ' ' || last_name as full_name
    from base_customers
),

orders as (
    select 
        id as order_id,
        user_id as customer_id,
        order_date,
        status as order_status,
        row_number() over (partition by user_id order by order_date, id) as user_order_seq
    from base_orders
),

-- Marts
customer_order_history as (
    select 
        cn.customer_id,
        cn.full_name,
        cn.surname,
        cn.givenname,

        min(o.order_date) as first_order_date,

        min(case 
            when o.order_status not in ('returned','return_pending') 
            then o.order_date 
        end) as first_non_returned_order_date,

        max(case 
            when o.order_status not in ('returned','return_pending') 
            then o.order_date 
        end) as most_recent_non_returned_order_date,

        coalesce(max(o.user_order_seq),0) as order_count,

        coalesce(count(case 
            when o.order_status != 'returned' 
            then 1 end), 0) as non_returned_order_count,

        sum(case 
            when o.order_status not in ('returned','return_pending') 
            then round(p.amount/100.0,2) else 0 
        end) as total_lifetime_value,

        sum(case 
            when o.order_status not in ('returned','return_pending') 
            then round(p.amount/100.0,2) else 0 
        end) / nullif(count(case 
            when o.order_status not in ('returned','return_pending') 
            then 1 end), 0) as avg_non_returned_order_value,

        array_agg(distinct o.order_id) as order_ids

    from orders o
    join customer_names cn on o.customer_id = cn.customer_id
    left join payments p on o.order_id = p.order_id

    where o.order_status not in ('pending') and p.status != 'fail'

    group by cn.customer_id, cn.full_name, cn.surname, cn.givenname
),

-- Final CTEs
final as (
    select 
        o.order_id,
        o.customer_id,
        cn.surname,
        cn.givenname,
        coh.first_order_date,
        coh.order_count,
        coh.total_lifetime_value,
        round(p.amount/100.0,2) as order_value_dollars,
        o.order_status,
        p.status as payment_status

    from orders o
    join customer_names cn on o.customer_id = cn.customer_id
    join customer_order_history coh on o.customer_id = coh.customer_id
    left join payments p on o.order_id = p.order_id

    where p.status != 'fail'
)

-- Simple Select Statement
select * from final