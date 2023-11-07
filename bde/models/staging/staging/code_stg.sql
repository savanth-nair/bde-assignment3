{{
    config(
        unique_key='id'
    )
}}

with

source  as (

    select * from {{ source('raw', 'nsw_lga_code') }}

),

lower_case as (
    select
        lga_code,
        lower(lga_name) as lga_name 
    from source
)


select * from lower_case
