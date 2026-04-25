/*
===============================================================================
RFM Customer Segmentation
===============================================================================
Author  : Wajahat Haider
Layer   : Gold (Analytics)
Purpose : Segment customers into behavioral tiers using Recency, Frequency,
          and Monetary (RFM) scoring.

Segments produced:
  Champions    — bought recently, buy often, spend the most
  Loyal        — regular buyers with high frequency
  At Risk      — used to buy often but haven't recently
  Lost         — low recency, frequency, and monetary value

Usage:
  Run directly against the Gold layer views after Silver load is complete.
===============================================================================
*/

WITH rfm_raw AS (
    -- Aggregate per-customer sales metrics from the fact table
    SELECT
        c.customer_id,
        c.first_name + ' ' + c.last_name   AS customer_name,
        c.country,
        MAX(f.order_date)                   AS last_order_date,
        DATEDIFF(DAY, MAX(f.order_date),
                 CAST(GETDATE() AS DATE))   AS recency_days,
        COUNT(DISTINCT f.order_number)      AS frequency,
        SUM(f.sales_amount)                 AS monetary
    FROM gold.fact_sales   f
    JOIN gold.dim_customers c ON c.customer_key = f.customer_key
    WHERE f.order_date IS NOT NULL
    GROUP BY c.customer_id, c.first_name, c.last_name, c.country
),
rfm_scores AS (
    -- Score each dimension 1-5 using NTILE quintiles
    SELECT
        *,
        NTILE(5) OVER (ORDER BY recency_days  ASC)  AS r_score,  -- lower days = better
        NTILE(5) OVER (ORDER BY frequency     DESC) AS f_score,
        NTILE(5) OVER (ORDER BY monetary      DESC) AS m_score
    FROM rfm_raw
),
rfm_combined AS (
    SELECT
        *,
        r_score + f_score + m_score AS rfm_total
    FROM rfm_scores
)
SELECT
    customer_id,
    customer_name,
    country,
    last_order_date,
    recency_days,
    frequency,
    ROUND(monetary, 2)   AS lifetime_value,
    r_score,
    f_score,
    m_score,
    rfm_total,
    -- Segment label based on combined score thresholds
    CASE
        WHEN rfm_total >= 13                          THEN 'Champions'
        WHEN rfm_total >= 10                          THEN 'Loyal'
        WHEN r_score   <= 2 AND f_score >= 3          THEN 'At Risk'
        WHEN rfm_total <= 6                           THEN 'Lost'
        ELSE 'Potential'
    END AS segment
FROM rfm_combined
ORDER BY rfm_total DESC;
