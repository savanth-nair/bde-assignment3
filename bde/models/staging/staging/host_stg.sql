{{
    config(
        unique_key='id'
    )
}}

with

source  as (

    select * from {{ ref('host_snapshot') }}

),

clean as (
    select
        id,
        host_id,
        host_name,
        TO_DATE(host_since, 'DD/MM/YYYY') AS host_since,
        host_is_superhost,
        lower(host_neighbourhood) as host_neighbourhood,
        dbt_scd_id,
        TO_DATE(dbt_updated_at, 'DD/MM/YYYY') AS dbt_updated_at,
        TO_DATE(dbt_valid_from, 'DD/MM/YYYY') AS dbt_valid_from,
        dbt_valid_to
    from source
    where host_name <> '' and host_since <> ''
)


select * from clean
