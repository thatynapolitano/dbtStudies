with customers as (
    select * from {{ ref('stg_jaffle_shop_customers') }}
),

orders as (
    select * from {{ ref('stg_jaffle_shop_orders') }}
),

final as (
    select 
        customers.id as customer_id,
        customers.first_name,
        customers.last_name,
        orders.id,
        orders.order_date

    from customers
    left join orders
        on customers.id = orders.id
)

select * from final



