WITH dm_listing_neighbourhood AS (
    SELECT
        l.listing_neighbourhood,
        TO_CHAR(l.scraped_date, 'MM/YYYY') AS month_year,
        ROUND(
            SUM(CASE WHEN p.has_availability = true THEN 1 ELSE 0 END)::NUMERIC / NULLIF(COUNT(p.has_availability), 0) * 100,
            2
        ) AS Active_listings_ratio,
        MIN(CASE WHEN p.has_availability = true THEN p.price END) AS minimum_price,
        MAX(CASE WHEN p.has_availability = true THEN p.price END) AS maximum_price,
        ROUND(
            PERCENTILE_CONT(0.5) WITHIN GROUP (
                ORDER BY CASE WHEN p.has_availability = true THEN p.price END
            )::NUMERIC,
            2
        ) AS median_price,
        ROUND(AVG(CASE WHEN p.has_availability = true THEN p.price END), 2) AS average_price,
        COUNT(DISTINCT host_id) AS distinct_host,
        ROUND(
            SUM(CASE WHEN h.host_is_superhost = true THEN 1 ELSE 0 END)::NUMERIC / NULLIF(COUNT(h.host_is_superhost), 0) * 100,
            2
        ) AS Superhost_ratio,
        ROUND(AVG(CASE WHEN p.has_availability = true THEN r.review_scores_rating END), 2) AS average_review_scores_rating,
        ROUND(
            (LEAD(
                SUM(CASE WHEN p.has_availability = true THEN 1 ELSE 0 END) 
                OVER (
                    PARTITION BY listing_neighbourhood 
                    ORDER BY TO_CHAR(l.scraped_date, 'MM/YYYY')
                )
            ) - SUM(CASE WHEN p.has_availability = true THEN 1 ELSE 0 END))::NUMERIC 
            / NULLIF(SUM(CASE WHEN p.has_availability = true THEN 1 ELSE 0 END), 0)::NUMERIC * 100,
            2
        ) AS active_change,
        ROUND(
            (LEAD(
                SUM(CASE WHEN p.has_availability = false THEN 1 ELSE 0 END) 
                OVER (
                    PARTITION BY listing_neighbourhood 
                    ORDER BY TO_CHAR(l.scraped_date, 'MM/YYYY')
                )
            ) - SUM(CASE WHEN p.has_availability = false THEN 1 ELSE 0 END))::NUMERIC 
            / NULLIF(SUM(CASE WHEN p.has_availability = true THEN 1 ELSE 0 END), 0)::NUMERIC * 100,
            2
        ) AS inactive_change,
        SUM(CASE WHEN p.has_availability = true THEN 30 - p.availability_30 ELSE 0 END) AS stays,
        ROUND(AVG(CASE WHEN p.has_availability = true THEN (30 - p.availability_30) * p.price ELSE 0 END), 2) AS avg_estimate_revenue
    FROM
        {{ ref('dim_property') }} p
    JOIN
        {{ ref('dim_listing') }} l ON p.id = l.id
    JOIN
        {{ ref('dim_host') }} h ON p.id = h.id
    JOIN
        {{ ref('fact_review') }} r ON p.id = r.id
    GROUP BY
        l.listing_neighbourhood,
        TO_CHAR(l.scraped_date, 'MM/YYYY')
    ORDER BY
        l.listing_neighbourhood,
        TO_CHAR(l.scraped_date, 'MM/YYYY')
)

SELECT * FROM dm_listing_neighbourhood;
