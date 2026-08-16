{{ config(materialized='incremental', unique_key='order_id', incremental_strategy='merge') }}

select
    order_id,
    customer_id,
    product_id,
    store_id,
    order_date,
    order_timestamp,
    quantity,
    unit_price,
    total_amount,
    updated_at
from {{ ref('stg_current_orders') }}

{% if is_incremental() %}
where updated_at > (select coalesce(max(updated_at), timestamp('1900-01-01')) from {{ this }})
{% endif %}

