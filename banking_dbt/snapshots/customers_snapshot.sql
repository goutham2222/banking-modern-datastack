{% snapshot customers_snapshot %}

{{
    config(
        target_schema='ANALYTICS',
        unique_key='customer_id',
        strategy='check',
        check_cols=['first_name', 'last_name', 'email'],
        updated_at='load_timestamp'
    )
}}

select *
from {{ ref('stg_customers') }}

{% endsnapshot %}