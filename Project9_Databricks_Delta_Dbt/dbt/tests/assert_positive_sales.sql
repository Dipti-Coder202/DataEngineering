select *
from {{ ref('fct_sales') }}
where quantity <= 0 or unit_price < 0 or total_amount < 0

