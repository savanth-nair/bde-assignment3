WITH dm_host_neighbourhood AS (
    SELECT 
        l.id, 
        l.scraped_date, 
        h.host_id,
        h.host_name,
        h.host_neighbourhood,
        s.lga_name,
        p.price,
        p.has_availability,
        p.availability_30 
    FROM 
        {{ ref('dim_listing') }} l 
    JOIN 
        {{ ref('dim_host') }} h
        ON l.id = h.id
    JOIN 
        {{ ref('dim_property') }} p
        ON l.id = p.id 
    JOIN
        {{ ref('dim_lga') }} s
        ON lower(h.host_neighbourhood) = lower(s.suburb_name) 
)

SELECT 
    lga_name AS host_neighbourhood_lga,
    TO_CHAR(scraped_date, 'MM/YYYY') AS month_year,
    COUNT(DISTINCT host_id) AS distinct_host,
    ROUND(
        SUM(
            CASE WHEN has_availability = true THEN 30 - availability_30 ELSE 0 END * price
        ), 2
    ) AS estimate_revenue,
    ROUND(
        SUM(
            CASE WHEN has_availability = true THEN 30 - availability_30 ELSE 0 END * price
        ) / NULLIF(COUNT(DISTINCT host_id), 0)::NUMERIC, 2
    ) AS per_host_estimate_revenue
FROM 
    dm_host_neighbourhood
GROUP BY 
    host_neighbourhood_lga,
    TO_CHAR(scraped_date, 'MM/YYYY')
ORDER BY 
    host_neighbourhood_lga,
    TO_CHAR(scraped_date, 'MM/YYYY');
