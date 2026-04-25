/*
===============================================================================
Month-over-Month Revenue Trends
===============================================================================
Author  : Wajahat Haider
Layer   : Gold (Analytics)
Purpose : Track monthly revenue, calculate month-over-month growth percentage,
          and compute a 3-month rolling average to smooth seasonal noise.

Useful for identifying peak sales periods and sustained growth/decline trends.
===============================================================================
*/

WITH monthly AS (
    SELECT
        YEAR(order_date)                    AS yr,
        MONTH(order_date)                   AS mo,
        DATEFROMPARTS(
            YEAR(order_date),
            MONTH(order_date), 1)           AS month_start,
        SUM(sales_amount)                   AS revenue,
        COUNT(DISTINCT order_number)        AS order_count,
        COUNT(DISTINCT customer_key)        AS unique_customers
    FROM gold.fact_sales
    WHERE order_date IS NOT NULL
    GROUP BY YEAR(order_date), MONTH(order_date)
),
with_lag AS (
    SELECT
        *,
        LAG(revenue) OVER (ORDER BY month_start) AS prev_revenue
    FROM monthly
)
SELECT
    month_start,
    FORMAT(month_start, 'MMM yyyy')         AS month_label,
    revenue,
    order_count,
    unique_customers,
    ROUND(
        100.0 * (revenue - prev_revenue)
              / NULLIF(prev_revenue, 0),
    2)                                      AS mom_growth_pct,
    ROUND(
        AVG(revenue) OVER (
            ORDER BY month_start
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),
    2)                                      AS rolling_3m_avg
FROM with_lag
ORDER BY month_start;
