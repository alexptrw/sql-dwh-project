/*
========================================================================================
Scripts ran to get a better understanding of the data
========================================================================================
Sciprt purpose
  This script shows individual queries ran to get key metrics in undestanding the data from the gold layer
========================================================================================
*/

SELECT * FROM INFORMATION_SCHEMA.COLUMNS
--WHERE TABLE_NAME = 'dim_customers'

-- Summary of key metrics 
SELECT 'Total Sales' as measure_name, SUM(sales_amount) as measure_value
FROM gold.fact_sales
UNION ALL
SELECT 'Total Quantyty' as measure_name, SUM(quantity) as measure_value
FROM gold.fact_sales
UNION ALL
SELECT 'Avg Price' as measure_name, AVG(price) as measure_value
FROM gold.fact_sales
UNION ALL
SELECT 'Total Num Orders' as measure_name, COUNT(DISTINCT(order_number)) as measure_value
FROM gold.fact_sales
UNION ALL
SELECT 'Total Num Products' as measure_name, COUNT(DISTINCT(products_key)) as measure_value
FROM gold.fact_sales
UNION ALL
SELECT 'Total Num Customers' as measure_name, COUNT(cusotmer_key) as measure_value
FROM gold.fact_sales

-- Overview of country distribution among customers
SELECT country, COUNT(customer_number) as total_customer FROM gold.dim_customers
GROUP BY country
ORDER BY total_customer DESC

-- Overview of gender distribution among customers
SELECT gender, COUNT(customer_number) as total_customer FROM gold.dim_customers
GROUP BY gender
ORDER BY total_customer DESC 

-- Total Products by category 
SELECT category, COUNT(products_key) as total_products FROM gold.dim_products
GROUP BY category
ORDER BY total_products DESC

-- Average cost per category
SELECT category, AVG(cost) as avg_cost FROM gold.dim_products
GROUP BY category
ORDER BY avg_cost DESC 

- Total Revenue by category
SELECT p.category, SUM(s.sales_amount) as total_rev FROM gold.dim_products p
RIGHT JOIN gold.fact_sales s ON p.products_key = s.products_key
GROUP BY p.category
ORDER BY total_rev DESC 


- Total Revenue by customer
SELECT c.cusotmer_key, c.firstname, c.lastname, SUM(s.sales_amount) as total_rev FROM gold.dim_customers c
RIGHT JOIN gold.fact_sales s ON c.cusotmer_key = s.cusotmer_key
GROUP BY c.cusotmer_key, c.firstname, c.lastname
ORDER BY total_rev DESC 

-- Items sold per country
SELECT
SUM(p.quantity) as items_sold,
c.country
FROM gold.dim_customers c
JOIN gold.fact_sales p ON p.cusotmer_key = c.cusotmer_key
GROUP BY country
ORDER BY items_sold DESC

-- Overview of categories by country + total items sold per cat
SELECT
DISTINCT prd.category,
c.country,
SUM(p.quantity) as items_sold
FROM gold.dim_customers c
JOIN gold.fact_sales p ON p.cusotmer_key = c.cusotmer_key
INNER JOIN gold.dim_products prd ON prd.products_key = p.products_key
GROUP BY country, prd.category
ORDER BY country


-- Top 5 producst by rev
SELECT TOP(10)* FROM gold.dim_products
SELECT TOP(10)* FROM gold.dim_customers
SELECT TOP(10)* FROM gold.fact_sales

SELECT TOP 5 p.product_name, 
SUM(s.sales_amount) as total_rev 
FROM gold.dim_products p
RIGHT JOIN gold.fact_sales s ON p.products_key = s.products_key
GROUP BY p.product_name
ORDER BY total_rev DESC 

-- Top 5 producst by rev
SELECT * FROM (
SELECT p.product_name, 
SUM(s.sales_amount) as total_rev,
ROW_NUMBER() OVER (ORDER BY SUM(s.sales_amount) DESC) rank_products
FROM gold.dim_products p
RIGHT JOIN gold.fact_sales s ON p.products_key = s.products_key
GROUP BY p.product_name) t
WHERE rank_products <= 5

-- Least 5 producst by rev
SELECT TOP 5 p.product_name, 
SUM(s.sales_amount) as total_rev 
FROM gold.dim_products p
RIGHT JOIN gold.fact_sales s ON p.products_key = s.products_key
GROUP BY p.product_name
ORDER BY total_rev 
