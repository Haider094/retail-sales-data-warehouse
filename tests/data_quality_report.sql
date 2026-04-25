/*
===============================================================================
Data Quality Report
===============================================================================
Author  : Wajahat Haider
Purpose : Comprehensive automated quality checks across Bronze, Silver, and
          Gold layers. Run after each layer load to catch issues early.

Checks performed:
  Bronze  — row counts and null key detection
  Silver  — nulls, duplicates, out-of-range values, referential integrity
  Gold    — surrogate key uniqueness, orphaned fact rows

Expected result for a clean load: every check returns 0 rows.
===============================================================================
*/

PRINT '================================================================';
PRINT 'DATA QUALITY REPORT';
PRINT 'Run date: ' + CONVERT(VARCHAR, GETDATE(), 120);
PRINT '================================================================';

-- ============================================================
-- SECTION 1: BRONZE LAYER — Row Counts & Null Key Check
-- ============================================================
PRINT '';
PRINT '--- BRONZE: Row Counts ---';

SELECT 'bronze.crm_cust_info'    AS table_name, COUNT(*) AS row_count FROM bronze.crm_cust_info    UNION ALL
SELECT 'bronze.crm_prd_info',                   COUNT(*) FROM bronze.crm_prd_info                  UNION ALL
SELECT 'bronze.crm_sales_details',              COUNT(*) FROM bronze.crm_sales_details             UNION ALL
SELECT 'bronze.erp_cust_az12',                  COUNT(*) FROM bronze.erp_cust_az12                 UNION ALL
SELECT 'bronze.erp_loc_a101',                   COUNT(*) FROM bronze.erp_loc_a101                  UNION ALL
SELECT 'bronze.erp_px_cat_g1v2',                COUNT(*) FROM bronze.erp_px_cat_g1v2;

PRINT '--- BRONZE: Null primary keys (expect 0) ---';

SELECT 'bronze.crm_cust_info — null cst_id' AS check_name,
       COUNT(*) AS violation_count
FROM bronze.crm_cust_info
WHERE cst_id IS NULL
UNION ALL
SELECT 'bronze.crm_sales_details — null order num',
       COUNT(*)
FROM bronze.crm_sales_details
WHERE sls_ord_num IS NULL;


-- ============================================================
-- SECTION 2: SILVER LAYER — Data Quality Checks
-- ============================================================
PRINT '';
PRINT '--- SILVER: Duplicate customer IDs (expect 0) ---';

SELECT cst_id, COUNT(*) AS duplicate_count
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1;

PRINT '--- SILVER: Negative or NULL product costs (expect 0) ---';

SELECT prd_id, prd_nm, prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

PRINT '--- SILVER: Sales amount inconsistency — sales != qty * price (expect 0) ---';

SELECT sls_ord_num, sls_quantity, sls_price, sls_sales,
       sls_quantity * sls_price AS expected_sales
FROM silver.crm_sales_details
WHERE ABS(sls_sales - sls_quantity * sls_price) > 1  -- allow $1 rounding tolerance
   OR sls_sales    IS NULL
   OR sls_quantity IS NULL
   OR sls_price    IS NULL
   OR sls_sales    <= 0
   OR sls_quantity <= 0
   OR sls_price    <= 0;

PRINT '--- SILVER: Order date after ship/due date (expect 0) ---';

SELECT sls_ord_num, sls_order_dt, sls_ship_dt, sls_due_dt
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
   OR sls_order_dt > sls_due_dt;

PRINT '--- SILVER: Future or ancient birthdates (expect 0) ---';

SELECT cid, bdate
FROM silver.erp_cust_az12
WHERE bdate > GETDATE()
   OR bdate < '1924-01-01';

PRINT '--- SILVER: Whitespace in string fields (expect 0) ---';

SELECT 'crm_cust_info — cst_key has leading/trailing space' AS issue, COUNT(*) AS cnt
FROM silver.crm_cust_info WHERE cst_key != TRIM(cst_key)
UNION ALL
SELECT 'crm_prd_info — prd_nm has leading/trailing space', COUNT(*)
FROM silver.crm_prd_info WHERE prd_nm != TRIM(prd_nm);

PRINT '--- SILVER: Unmapped gender/marital status values ---';

SELECT DISTINCT cst_gndr           FROM silver.crm_cust_info;
SELECT DISTINCT cst_marital_status FROM silver.crm_cust_info;
SELECT DISTINCT gen                FROM silver.erp_cust_az12;

PRINT '--- SILVER: Country code standardization check ---';

SELECT DISTINCT cntry FROM silver.erp_loc_a101 ORDER BY cntry;


-- ============================================================
-- SECTION 3: GOLD LAYER — Integrity Checks
-- ============================================================
PRINT '';
PRINT '--- GOLD: Duplicate surrogate keys in dim_customers (expect 0) ---';

SELECT customer_key, COUNT(*) AS dup_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

PRINT '--- GOLD: Duplicate surrogate keys in dim_products (expect 0) ---';

SELECT product_key, COUNT(*) AS dup_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

PRINT '--- GOLD: Orphaned fact rows — no matching dimension (expect 0) ---';

SELECT f.order_number,
       f.customer_key,
       f.product_key,
       CASE WHEN c.customer_key IS NULL THEN 'Missing customer' ELSE '' END AS customer_issue,
       CASE WHEN p.product_key  IS NULL THEN 'Missing product'  ELSE '' END AS product_issue
FROM gold.fact_sales   f
LEFT JOIN gold.dim_customers c ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products  p ON p.product_key  = f.product_key
WHERE c.customer_key IS NULL
   OR p.product_key  IS NULL;

PRINT '--- GOLD: Negative sales amounts (expect 0) ---';

SELECT order_number, sales_amount
FROM gold.fact_sales
WHERE sales_amount <= 0 OR sales_amount IS NULL;

PRINT '================================================================';
PRINT 'Report complete. Resolve any rows returned above before use.';
PRINT '================================================================';
