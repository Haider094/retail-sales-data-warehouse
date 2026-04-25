/*
===============================================================================
KPI Summary Dashboard Query
===============================================================================
Author  : Wajahat Haider
Layer   : Gold (Analytics)
Purpose : Single query that produces a flat KPI row suitable for a BI
          dashboard tile or executive summary report.

KPIs returned:
  - Total Revenue
  - Average Order Value (AOV)
  - Total Orders
  - Unique Customers
  - Average Items per Order
  - Best Sales Month (by revenue)
  - Top Product (by revenue)
  - Top Country (by revenue)
===============================================================================
*/

WITH sales_base AS (
    SELECT
        f.order_number,
        f.order_date,
        f.sales_amount,
        f.quantity,
        p.product_name,
        c.country
    FROM gold.fact_sales    f
    JOIN gold.dim_products  p ON p.product_key  = f.product_key
    JOIN gold.dim_customers c ON c.customer_key = f.customer_key
    WHERE f.order_date IS NOT NULL
),
monthly_rev AS (
    SELECT
        FORMAT(order_date, 'MMM yyyy') AS month_label,
        SUM(sales_amount)              AS rev
    FROM sales_base
    GROUP BY FORMAT(order_date, 'MMM yyyy')
),
top_product AS (
    SELECT TOP 1 product_name, SUM(sales_amount) AS rev
    FROM sales_base
    GROUP BY product_name
    ORDER BY rev DESC
),
top_country AS (
    SELECT TOP 1 country, SUM(sales_amount) AS rev
    FROM sales_base
    GROUP BY country
    ORDER BY rev DESC
),
best_month AS (
    SELECT TOP 1 month_label
    FROM monthly_rev
    ORDER BY rev DESC
)
SELECT
    ROUND(SUM(s.sales_amount), 2)                                AS total_revenue,
    ROUND(SUM(s.sales_amount) / NULLIF(COUNT(DISTINCT s.order_number), 0), 2)
                                                                 AS avg_order_value,
    COUNT(DISTINCT s.order_number)                               AS total_orders,
    COUNT(DISTINCT c.customer_key)                               AS unique_customers,
    ROUND(CAST(SUM(s.quantity) AS FLOAT)
          / NULLIF(COUNT(DISTINCT s.order_number), 0), 1)        AS avg_items_per_order,
    (SELECT month_label FROM best_month)                         AS best_sales_month,
    (SELECT product_name FROM top_product)                       AS top_product,
    (SELECT country       FROM top_country)                      AS top_country
FROM gold.fact_sales    s
JOIN gold.dim_customers c ON c.customer_key = s.customer_key
WHERE s.order_date IS NOT NULL;
