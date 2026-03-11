{% snapshot locations_snapshot %}

{{
    config(
        target_schema='ANALYTICS',
        unique_key='zip_code',
        strategy='check',
        check_cols=['city', 'state_code', 'state_name'],
        updated_at='load_timestamp'
    )
}}

select * from {{ ref('stg_locations') }}

{% endsnapshot %}