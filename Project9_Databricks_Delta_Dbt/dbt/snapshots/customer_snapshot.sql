{% snapshot customer_snapshot %}

{{
  config(
    target_schema='snapshots',
    unique_key='customer_id',
    strategy='check',
    check_cols=['customer_name', 'customer_email']
  )
}}

select customer_id, customer_name, customer_email
from {{ ref('dim_customer') }}

{% endsnapshot %}

