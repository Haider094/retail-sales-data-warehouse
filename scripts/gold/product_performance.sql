/*
===============================================================================
Product Performance Analysis
===============================================================================
Author  : Wajahat Haider
Layer   : Gold (Analytics)
Purpose : Rank all products by revenue and highlight top 10 / bottom 10
          performers. Includes each product's percentage contribution to
          total sales — useful for assortment and pricing decisions.
===============================================================================
*/

WITH product_sales AS (
    SELECT
        p.product_name,
        p.category,
        p.subcategory,
        p.product_line,
        SUM(f.sales_amount)            AS total_revenue,
        SUM(f.quantity)                AS units_sold,
        COUNT(DISTINCT f.order_number) AS order_count,
        ROUND(AVG(f.price), 2)         AS avg_selling_price,
        p.cost                         AS unit_cost
    FROM gold.fact_sales   f
    JOIN gold.dim_products p ON p.product_key = f.product_key
    WHERE f.order_date IS NOT NULL
    GROUP BY
        p.product_name, p.category, p.subcategory,
        p.product_line, p.cost
),
ranked AS (
    SELECT
        *,
        ROUND(100.0 * total_revenue / SUM(total_revenue) OVER (), 2) AS pct_of_total,
        RANK() OVER (ORDER BY total_revenue DESC)                      AS revenue_rank,
        -- Estimated margin: (avg price - cost) / avg price
        ROUND(100.0 * (avg_selling_price - unit_cost)
              / NULLIF(avg_selling_price, 0), 1)                       AS est_margin_pct
    FROM product_sales
)
-- Top 10
SELECT 'Top 10'      AS tier, * FROM ranked WHERE revenue_rank <= 10
UNION ALL
-- Bottom 10 (exclude products with zero revenue)
SELECT 'Bottom 10'   AS tier, * FROM ranked
WHERE revenue_rank > (SELECT COUNT(*) FROM ranked) - 10
  AND total_revenue > 0
ORDER BY tier DESC, revenue_rank;
