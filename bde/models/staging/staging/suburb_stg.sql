{{
    config(
        unique_key='id'
    )
}}

with

source  as (

    select * from {{ source('raw', 'nsw_lga_suburb') }}

),

lower_case as (
    select
        lower(lga_name) as lga_name,
        lower(suburb_name) as suburb_name
    from source
)


select * from lower_case
