select
    f.order_date,
    f.store_id,
    p.category,
    count(distinct f.order_id) as order_count,
    sum(f.quantity) as units_sold,
    cast(sum(f.total_amount) as decimal(18, 2)) as gross_sales,
    cast(avg(f.total_amount) as decimal(18, 2)) as average_order_value
from {{ ref('fct_sales') }} f
left join {{ ref('dim_product') }} p using (product_id)
group by f.order_date, f.store_id, p.category

