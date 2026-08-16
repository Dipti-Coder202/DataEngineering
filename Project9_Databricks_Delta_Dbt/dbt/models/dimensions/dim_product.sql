select
    product_id,
    max_by(product_name, updated_at) as product_name,
    max_by(category, updated_at) as category,
    max(updated_at) as last_updated_at
from {{ ref('stg_current_orders') }}
group by product_id

