-- Filename: fact_listing.sql

with fact_listing as (
    select
        l.id as listing_id,
        l.listing_id as original_listing_id,
        l.scraped_date,
        h.host_id,
        h.host_name,
        h.host_neighbourhood,
        l.listing_neighbourhood,
        p.property_type,
        p.room_type,
        p.accommodates,
        p.price,
        p.has_availability,  
        p.availability_30,
        p.number_of_reviews,
        p.review_scores_rating,
        s.lga_name as listing_neighbourhood_lga,
        s.suburb_name as listing_suburb
    from 
        {{ ref('dim_listing') }} l
    join 
        {{ ref('dim_host') }} h
    on 
        l.id = h.id
    join 
        {{ ref('dim_property') }} p
    on 
        l.id = p.id
    left join 
        {{ ref('dim_lga') }} s
    on 
        lower(l.listing_neighbourhood) = lower(s.suburb_name) 
)

select * from fact_listing
