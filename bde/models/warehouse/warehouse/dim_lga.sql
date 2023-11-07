{{
    config(
        unique_key='lga_code'
    )
}}

select c.lga_code, s.lga_name, s.suburb_name from {{ ref('code_stg') }} c 
join {{ ref('suburb_stg') }} s 
on c.lga_name = s.lga_name
