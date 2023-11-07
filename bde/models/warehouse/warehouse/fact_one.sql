{{
    config(
        unique_key='id'
    )
}}

select * from {{ source('raw', 'census_one') }}
