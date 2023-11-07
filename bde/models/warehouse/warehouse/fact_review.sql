{{
    config(
        unique_key='id'
    )
}}

select * from {{ ref('review_snapshot') }}
