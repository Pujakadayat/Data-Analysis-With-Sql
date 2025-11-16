use gold

-- Analyze Sales Performance Over Time

-- Select 
-- datetrunc(month,order_date) as order_year,
-- sum(sales_amount) total_Sales,
-- count(distinct customer_key) as total_customers,
-- sum(quantity) as total_quantity
-- from gold_fact_sales
-- where order_date is not null and order_date != ''
-- group by datetrunc(month,order_date)
-- order by datetrunc(month,rder_date)

-- OR 

Select 
year(order_date) as order_year,
month(order_date) as order_month,
sum(sales_amount) total_Sales,
count(distinct customer_key) as total_customers,
sum(quantity) as total_quantity
from gold_fact_sales
where order_date is not null and order_date != ''
group by year(order_date),month(order_date)
order by year(order_date),month(order_date)


-- CUMULATIVE ANALYSIS
-- Calculate the total sales per month  and the running total of sales over time

select 
order_date,
total_sales,
sum(total_sales) over(order by order_date) as running_total_sales,
avg(avg_price) over(order by order_date) as moving_average_price
from (
select
datetrunc(month,order_date) order_date,
sum(sales_amount) total_Sales,
Avg(price) as avg_price
from gold_fact_sales
where order_date is not null and order_date !=''
group by datetrunc(month,order_date)
order by datetrunc(month,order_date)
)t

-- Performance Analysis

-- Anlayze the yearly performance of products by comparing each products sales to both
-- its average sales performance and the previous year's sales

	with yearly_product_sales as(
	select
	year(f.order_date) as order_year,
	p.product_name,
	sum(f.sales_amount) as current_sales
	from gold_fact_sales f
	left join gold_dim_products p
	on f.product_key = p.product_key
	where f.order_date is not null and f.order_date !=''
	group by year(f.order_Date),
	p.product_name
	)

	select 
	order_year,
	product_name,
	current_sales,
	avg(current_sales) over(partition by product_name) avg_sales
	current_sales - avg(current_sales) over(partition by product_name) diff_avg

	case 
	when current_sales - avg(current_sales) over(partition by product_name) diff_avg > 0 Then 'Above Average'
	when current_sales - avg(current_sales) over(partition by product_name) diff_avg < 0 Then 'Below Average'
	Else 'Avg'
	End avg_changes,
    
--     Year over Year Analysis
    
    lag(current_sales) over(partition by product_name order by order_year) previous_year_sales
	current_sales - lag(current_sales) over(partition by product_name order by order_year) as diff_previous_year
    case 
	when current_sales - lag(current_sales) over(partition by product_name order by order_year) diff_avg > 0 Then 'Increase'
	when current_sales - lag(current_sales) over(partition by product_name order by order_year) diff_avg < 0 Then 'Decrease'
	Else 'No chnage'
	End py_changes,
    from yearly_product_sales
	order by product_name,order_year


-- Part - to - Whole (Proportional Analysis)

-- Which categories contribute the most to overall Sales
with category_sales as (
select 
category,
sum(sales_amount) total_sales
from gold_fact_Sales f
left join gold_dim_products p
on p.product_key = f.product_key
group by category 
)


select 
category,
total_sales ,
sum(total_sales) over() overall_sales,
Concat(Round((cast (total_Sales as float) / sum(total_Sales) over()) * 100,2),'%') as percentage_of_total
from category_sales
order by total_sales desc


-- Data Segmentation

-- Segment products  into cost ranges and count how many products fall into each segment

-- with product_segments as (
-- select 
-- product_key,
-- product_name,
-- cost,
-- case  
-- when  cost < 100 then 'below 100'
-- when  cost between 100 and 500 then '100-500'
-- when cost between 500 and 1000 then '500-1000'
-- else 'Above 1000'
-- end cost_range
-- from gold_dim_products )

-- select 
-- cost_range,
-- count(product_key) as total_products
-- from products_segments
-- group by cost_range
-- order by total_products desc

-- Or 


SELECT  
    cost_range, 
    COUNT(product_key) AS total_products
FROM (
    SELECT  
        product_key, 
        product_name, 
        cost, 
        CASE   
            WHEN cost < 100 THEN 'below 100'
            WHEN cost BETWEEN 100 AND 500 THEN '100-500'
            WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
            ELSE 'Above 1000'
        END AS cost_range
    FROM gold_dim_products
) AS product_segments
GROUP BY cost_range

-- Group customers into three segments based on their spending  behaviour

-- -VIP: at least 12 months of history  and spending more than $5,000.
-- -Regular: at least 12 months of history and spending $5,000 or less
-- - New : lifespan less than 12 months
	-- with customer_spending as (
-- 	select 
-- 	c.customer_key,
-- 	sum(f.sales_amount) as total_spending,
-- 	Min(order_date) first_order,
-- 	max(order_date) last_order,
-- 	datediff(month,min(order_date), max(order_date)  as lifespan
-- 	from gold_fact_sales f 
-- 	left join gold_dim_customers c
-- 	on f.customer_key = c.customer_key
-- 	group by c.customer_key )

-- select 
-- customer_segment,
-- count(customer_key)
-- from (
-- 	select 
--     customer_key,
-- 	case 
-- 	when lifespan >= 12 and total_spending > 5000 then  'VIP'
-- 	when lifespan <=12 and total_spending <=5000 then 'Regular'
-- 	Else 'New'
-- 	End customer_segment
-- 	from customer_spending ) t
--     group by customer_segment

SELECT 
    customer_segment,
    COUNT(customer_key) AS total_customers
FROM (
    SELECT 
        customer_key,
        total_spending,
        first_order,
        last_order,
        lifespan,
        CASE 
            WHEN lifespan >= 12 AND total_spending > 5000 THEN 'VIP'
            WHEN lifespan <= 12 AND total_spending <= 5000 THEN 'Regular'
            ELSE 'New'
        END AS customer_segment
    FROM (
        SELECT 
            c.customer_key,
            SUM(f.sales_amount) AS total_spending,
            MIN(f.order_date) AS first_order,
            MAX(f.order_date) AS last_order,
            TIMESTAMPDIFF(MONTH, MIN(f.order_date), MAX(f.order_date)) AS lifespan
        FROM gold_fact_sales f 
        LEFT JOIN gold_dim_customers c ON f.customer_key = c.customer_key
        GROUP BY c.customer_key
    ) AS customer_spending
) AS t
GROUP BY customer_segment



/*
===============================================================================
Product Report
===============================================================================
Customer Report
===============================================================================
Purpose:
    - This report consolidates key customer metrics and behaviors

Highlights:
    1. Gathers essential fields such as names, ages, and transaction details.
	2. Segments customers into categories (VIP, Regular, New) and age groups.
    3. Aggregates customer-level metrics:
	   - total orders
	   - total sales
	   - total quantity purchased
	   - total products
	   - lifespan (in months)
    4. Calculates valuable KPIs:
	    - recency (months since last order)
		- average order value
		- average monthly spend
===============================================================================
*/
-- =============================================================================
-- Create Report: gold.report_products
-- =============================================================================
CREATE VIEW gold_report_customers AS

with base_query as(
/*---------------------------------------------------------------------------
1) Base Query: Retrieves core columns from tables
---------------------------------------------------------------------------*/
select 
f.order_number,
f.product_key,
f.order_date,
f.sales_amount,
f.quantity,
c.customer_key,
c.customer_number,
concat(c.first_name,' ',c.last_name) as customer_name,
Datediff(year,c.birthdate,Getdate()) age
from gold_fact_Sales f
left join gold_dim_customers c
on c.customer_key = f.customer_key 
where order_date is not null and order_date != ''
), customer_aggregations as (
/*---------------------------------------------------------------------------
2) Customer Aggregations: Summarizes key metrics at the customer level
---------------------------------------------------------------------------*/
select 
customer_key,
customer_number,
customer_name,
age,
count(distinct order_number) as total_orders,
sum(sales_amount) as total_sales,
sum(quantity) as total_quantity,
count(distinct product_key) as total_products,
max(order_date) as last_order_date,
Datediff(month,min(order_date), max(order_date)) as lifespan
from base_query
group by 
customer_key,
customer_number,
customer_name,
age)


select
customer_key,
customer_number,
customer_name,
age,
case when age < 20 then 'Under 20'
when age between 20 and 29 then '20-29'
when age between 30 and 39 then '30-39'
when age between 40 and 49 then '40-49'
else '50 and above'
end as age_group,
	case 
	when lifespan >= 12 and total_spending > 5000 then  'VIP'
	when lifespan <=12 and total_spending <=5000 then 'Regular'
	Else 'New'
	End  as customer_segment,
    last_order_date,
    datediff(month,last_order_date,getdate()) as recency,
total_orders,
total_sales,
total_quantity,
total_products,
lifespan,

-- Compuate average order value (AVO)
CASE WHEN total_sales = 0 THEN 0
	 ELSE total_sales / total_orders
END AS avg_order_value,

-- Compuate average monthly spend
CASE WHEN lifespan = 0 THEN total_sales
     ELSE total_sales / lifespan
END AS avg_monthly_spend

from customer_aggregation


























