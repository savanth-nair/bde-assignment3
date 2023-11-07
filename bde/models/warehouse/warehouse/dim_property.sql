{{
    config(
        unique_key='id'
    )
}}

select * from {{ ref('property_stg') }}
