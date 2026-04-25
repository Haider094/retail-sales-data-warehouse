# Data Quality Findings

**Layer coverage:** Bronze → Silver → Gold  
**Script:** `tests/data_quality_report.sql`

---

## Bronze Layer Observations

| Table | Row Count |
|---|---|
| bronze.crm_cust_info | 18,484 |
| bronze.crm_prd_info | 397 |
| bronze.crm_sales_details | 60,398 |
| bronze.erp_cust_az12 | 18,148 |
| bronze.erp_loc_a101 | 18,484 |
| bronze.erp_px_cat_g1v2 | 37 |

**Issues found:**
- `crm_cust_info`: ~3% null values in `cst_gndr` — expected; handled in Silver with `n/a` fallback
- `crm_prd_info`: `prd_cost` NULL for 2 historical product records — cost defaulted to 0 in Silver
- `crm_sales_details`: `sls_order_dt` stored as integer (e.g. `20101229`) — cast to DATE in Silver

---

## Silver Layer Transformations Applied

### crm_cust_info
- Deduplicated on `cst_id` using `ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC)` — kept latest record per customer
- Mapped gender codes: `M → Male`, `F → Female`, unknown → `n/a`
- Mapped marital status: `M → Married`, `S → Single`, unknown → `n/a`
- Trimmed leading/trailing whitespace from `cst_firstname` and `cst_lastname`

### crm_sales_details
- Date integers converted to DATE type; malformed/zero values set to NULL
- 0 records with order_date after ship_date after fix
- Recalculated `sls_sales = sls_quantity * ABS(sls_price)` for rows where original value was NULL, zero, or inconsistent

### erp_cust_az12
- Stripped `NAS` prefix from `cid` to align with CRM customer keys
- 3 future birthdates set to NULL
- Gender normalized to `Male / Female / n/a`

### erp_loc_a101
- Removed `-` from `cid` values to match CRM format
- Country codes standardized: `DE → Germany`, `US / USA → United States`, blank → `n/a`

---

## Gold Layer Validation

| Check | Result |
|---|---|
| Duplicate customer_key in dim_customers | 0 rows |
| Duplicate product_key in dim_products | 0 rows |
| Orphaned fact rows (no matching dimension) | 0 rows |
| Negative sales amounts | 0 rows |

Gold layer passed all integrity checks after Silver transformations.

---

## Decisions Made

1. **Kept `n/a` as a sentinel value** (not NULL) for unknown gender/country so BI tools can filter or display it explicitly rather than treating unknowns as missing data.
2. **Used CRM as the primary source for gender**, falling back to ERP only when CRM value is `n/a` — CRM data is more current and complete.
3. **Excluded historical products** (`prd_end_dt IS NOT NULL`) from `dim_products` to keep the dimension current. Historical orders still join correctly because `fact_sales` was already loaded before the end date was applied.
