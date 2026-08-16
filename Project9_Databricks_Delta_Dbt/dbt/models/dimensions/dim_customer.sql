select
    customer_id,
    max_by(customer_name, updated_at) as customer_name,
    max_by(customer_email, updated_at) as customer_email,
    min(order_timestamp) as first_order_at,
    max(order_timestamp) as latest_order_at
from {{ ref('stg_current_orders') }}
group by customer_id

