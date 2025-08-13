with customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
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



