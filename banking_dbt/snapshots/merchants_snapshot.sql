{% snapshot merchants_snapshot %}

{{
    config(
        target_schema='ANALYTICS',
        unique_key='merchant_id',
        strategy='check',
        check_cols=['merchant_name', 'merchant_category'],
        updated_at='load_timestamp'
    )
}}

select * from {{ ref('stg_merchants') }}

{% endsnapshot %}