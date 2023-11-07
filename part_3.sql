-- Question 1
WITH population AS (
    SELECT 
        CAST(REPLACE(o.lga_code_2016, 'LGA', '') AS INTEGER) AS lga_code,
        o.age_0_4_yr_m + o.age_5_14_yr_m + o.age_15_19_yr_m AS population_under_20,
        o.age_0_4_yr_m + o.age_5_14_yr_m + o.age_15_19_yr_m + o.age_20_24_yr_m + o.age_25_34_yr_m AS population_under_35,
        ROUND(
            ((o.age_20_24_yr_m + o.age_25_34_yr_m)::NUMERIC / (o.age_0_4_yr_m + o.age_5_14_yr_m + o.age_15_19_yr_m + o.age_20_24_yr_m + o.age_25_34_yr_m)) * 100,
            2
        ) AS rate_20_34
    FROM 
        warehouse.fact_one o
),
listing AS (
    SELECT 
        l.listing_neighbourhood,
        ROUND(
            SUM(CASE WHEN p.has_availability = true THEN 30 - p.availability_30 ELSE 0 END * p.price),
            2
        ) AS estimate_revenue 
    FROM 
        warehouse.dim_listing l
    JOIN 
        warehouse.dim_property p
        ON p.id = l.id
    GROUP BY 
        l.listing_neighbourhood 
),
order_list AS (
    SELECT 
        a.lga_name,
        l.listing_neighbourhood,
        p.population_under_20,
        p.population_under_35,
        rate_20_34,
        l.estimate_revenue,
        ROW_NUMBER() OVER (ORDER BY l.estimate_revenue DESC) AS row_num,
        COUNT(*) OVER () AS total_rows
    FROM 
        warehouse.dim_lga a
    JOIN 
        population p 
        ON a.lga_code = p.lga_code
    JOIN 
        listing l 
        ON LOWER(l.listing_neighbourhood) = LOWER(a.suburb_name)
    ORDER BY 
        l.estimate_revenue DESC, 
        p.population_under_35 DESC
)

SELECT 
    lga_name,
    listing_neighbourhood,
    population_under_20,
    population_under_35,
    rate_20_34,
    estimate_revenue 
FROM 
    order_list
WHERE 
    row_num = 1 OR row_num = total_rows;


-- Question 2
with neighbourhood as(
select   
	l.listing_neighbourhood as listing_neighbourhood,
	round(sum(case when has_availability = true then 30 - availability_30  else 0 end * price),2) as estimate_revenue
 from warehouse.fact_listing l
 join 
 	warehouse.dim_host h
 on l.id = h.id
 join
 	warehouse.dim_property p 
 on p.id = l.id
 group by
 	l.listing_neighbourhood
 order by 
 	estimate_revenue desc
 limit 5
 ),
 rank_table as (
 select
 	l.listing_neighbourhood,
 	p.property_type, 
 	p.room_type,
 	l.accommodates,
 	(sum(case when l.has_availability = true then 30 - l.availability_30  else 0 end)) as stays,
    row_number() over (partition by l.listing_neighbourhood order by (sum(case when l.has_availability = true then 30 - l.availability_30  else 0 end)) desc) as row_num
 from warehouse.fact_listing l 
 join 
 	warehouse.dim_host h
 on l.id = h.id
 join
 	warehouse.dim_property p 
 on p.id = l.id
 join neighbourhood n
 on n.listing_neighbourhood = l.listing_neighbourhood
 group by l.listing_neighbourhood,p.property_type, p.room_type,l.accommodates 
 order by l.listing_neighbourhood desc, stays desc
 )
 select 
 	listing_neighbourhood,
 	property_type,
 	room_type,
 	accommodates,
 	stays
 from rank_table 
 where row_num = 1
 order by stays desc;




 --Question 3
WITH room_table AS (
    SELECT 
        dh.host_id, 
        dp.property_type,
        dp.room_type, 
        dp.accommodates  
    FROM 
        warehouse.dim_host dh 
    JOIN
        warehouse.dim_listing fl 
    ON dh.id = fl.id
    JOIN 
        warehouse.dim_property dp 
    ON dp.id = dh.id
    GROUP BY 
        dh.host_id, 
        dp.property_type,
        dp.room_type, 
        dp.accommodates
),
multiple_host AS ( 
    SELECT host_id 
    FROM room_table
    GROUP BY host_id
    HAVING count(host_id) > 1
),
host_area AS (
    SELECT 
        dh.id AS host_id, 
        dl.lga_name AS host_lga
    FROM 
        warehouse.dim_host dh 
    JOIN warehouse.dim_lga dl 
    ON lower(dh.host_neighbourhood) = lower(dl.suburb_name)
    WHERE dh.host_id IN (SELECT host_id FROM multiple_host)
),
listing_area AS (
    SELECT 
        fl.id AS listing_id, 
        dl.lga_name AS listing_lga
    FROM 
        warehouse.dim_listing fl 
    JOIN warehouse.dim_lga dl 
    ON lower(fl.listing_neighbourhood) = lower(dl.suburb_name)
)
SELECT 
    h.host_lga,
    COUNT(*) AS total_listings,
    SUM(CASE WHEN h.host_lga = l.listing_lga THEN 1 ELSE 0 END) AS local_listings,
    ROUND(SUM(CASE WHEN h.host_lga = l.listing_lga THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*),0)::numeric * 100,2) AS local_rate
FROM 
    host_area h
JOIN 
    listing_area l 
ON 
    h.host_id = l.listing_id
GROUP BY 
    h.host_lga
ORDER BY 
    local_rate DESC;




--Question 4


with room_table as (
select dh.host_id, dp.property_type,dp.room_type, dp.accommodates  from 
warehouse.dim_host dh 
join
warehouse.dim_listing fl 
on dh.id = fl.id
join 
warehouse.dim_property dp 
on dp.id = dh.id
group by dh.host_id, dp.property_type,dp.room_type, dp.accommodates
),
single_host as ( 
select host_id from room_table
group by host_id
having count(host_id) = 1
),
month_repay as (
select 
	cast(replace(lga_code_2016,'LGA','') as integer) as lga_code,
	ft.median_mortgage_repay_monthly as monthly_repay
from warehouse.fact_two ft 
),
lga_repay as (
select * 
from warehouse.dim_lga dl
join month_repay mr 
on mr.lga_code = dl.lga_code
),
host_area as (
select 
	sh.host_id,
	dh.host_name,
	fl.listing_neighbourhood,
	round(sum(case when has_availability = true then 30 - availability_30  else 0 end * price),2) as estimate_revenue 
from 
	single_host sh
join 
	warehouse.dim_host dh 
on sh.host_id = dh.host_id
join
	warehouse.dim_listing fl
on fl.id = dh.id 
join 
	warehouse.dim_property dp
on dp.id =dh.id 
group by sh.host_id, dh.host_name, fl.listing_neighbourhood 
),
revenue_table as (
select 
	ha.host_id,
	ha.host_name,
	ha.listing_neighbourhood,
	ha.estimate_revenue,
	lr.monthly_repay
from 
	host_area ha
join
	lga_repay lr
on lower(ha.listing_neighbourhood) = lower(lr.suburb_name)
order by estimate_revenue desc
)
select 
	round(
		sum(case when estimate_revenue > monthly_repay*12 then 1 else 0 end)::numeric /
		count(*)::numeric ,2
		) as covered
from revenue_table;







	