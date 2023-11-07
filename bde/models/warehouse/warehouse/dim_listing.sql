{{
    config(
        unique_key='id'
    )
}}

select * from {{ ref('listing_stg') }}
