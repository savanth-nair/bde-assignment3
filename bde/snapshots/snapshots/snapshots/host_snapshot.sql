{% snapshot host_snapshot %}

{{
        config(
          target_schema='raw',
          strategy='timestamp',
          unique_key='id',
          updated_at='host_since'
        )
    }}

select id,host_id,host_name,host_since,host_is_superhost,host_neighbourhood from {{ source('raw', 'house') }}

{% endsnapshot %}