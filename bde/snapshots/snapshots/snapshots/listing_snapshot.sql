{% snapshot listing_snapshot %}

{{
        config(
          target_schema='raw',
          strategy='timestamp',
          unique_key='id',
          updated_at='scraped_date'
        )
    }}

select id,listing_id,scraped_date,listing_neighbourhood from {{ source('raw', 'house') }}

{% endsnapshot %}