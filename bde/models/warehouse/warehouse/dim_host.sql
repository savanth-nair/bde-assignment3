{{
    config(
        unique_key='id'
    )
}}

select * from {{ ref('host_stg') }}
