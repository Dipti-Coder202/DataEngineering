with current_orders as (
    select *
    from {{ source('lakeflow', 'silver_orders_history') }}
    where __END_AT is null
)

select
    order_id,
    customer_id,
    customer_name,
    lower(customer_email) as customer_email,
    product_id,
    product_name,
    category,
    store_id,
    cast(order_timestamp as timestamp) as order_timestamp,
    cast(order_timestamp as date) as order_date,
    quantity,
    unit_price,
    total_amount,
    updated_at
from current_orders

