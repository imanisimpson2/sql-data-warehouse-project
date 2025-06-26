/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

--============================================== silver.crm_cust_info data quality checks
-- 1. Check for Nulls or Duplicates in Primary Key
-- A primary key must be unique and not null
-- Expectation: No result
SELECT
cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

-- 2. check for unwanted spaces
-- query each column individually
-- expectation: no results
-- finding duplicates in the primary key
SELECT
*
FROM (
SELECT
	*,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date) as flag_last
	FROM silver.crm_cust_info
)t WHERE flag_last != 1;
-- WHERE cst_id = 29466

-- 3. find trailing spaces
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

-- 4. data standardization & consistency
-- check the consistency of value in low cardinality columns
-- change gender f/m to full words as a rule
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info

--============================================== silver.crm_prd_info data quality checks
-- 1. Check for Nulls or Duplicates in Primary Key (prd_info)
-- Expectation: No result
SELECT
prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

-- 2. check for unwanted spaces
-- query each column individually, replace prd_nm
-- expectation: no results
-- finding duplicates in the primary key
SELECT prd_nm
FROM DataWarehouse.silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- 3. check for nulls or negative numbers
-- expectation: no results
SELECT prd_cost
FROM DataWarehouse.silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-- 4. check standardization & consistency of product lines
-- check all distinct possible values
SELECT DISTINCT prd_line
FROM DataWarehouse.silver.crm_prd_info

-- 5. check for invalid date orders
-- expectation: start date should come before end, therefore no result
-- end of first history should be younger than the start of the next record to avoid overlapping date
-- each record should always have a start date (no null in start date)
SELECT *
FROM DataWarehouse.silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt

--============================================== silver.crm_sales_details data quality checks
-- 1. trailing space check
SELECT 
	  sls_ord_num,
      sls_prd_key,
      sls_cust_id,
      sls_order_dt,
      sls_ship_dt,
      sls_due_dt,
      sls_sales,
      sls_quantity,
      sls_price
  FROM silver.crm_sales_details
  WHERE sls_ord_num != TRIM(sls_ord_num)

  -- 2. check if prd keys are matching with other table
  -- check if cust id matches with other table
  -- both expect no results
  SELECT 
	  sls_ord_num,
      sls_prd_key,
      sls_cust_id,
      sls_order_dt,
      sls_ship_dt,
      sls_due_dt,
      sls_sales,
      sls_quantity,
      sls_price
  FROM silver.crm_sales_details
 -- WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info)
 -- WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)

  -- 3. check for invalid dates (negative numbers, 0,)
  -- change integers to dates
  -- expects no result, use NULLIF to correct
  -- check if length of digits is 8 for transformation
  -- check if boundary of acceptable dates is surpassed
  -- shipping date check
  SELECT 
  NULLIF(sls_order_dt,0) sls_order_dt
  FROM silver.crm_sales_details
-- WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) != 8
  WHERE 
  sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8
-- OR sls_ship_dt > 20500101 OR sls_ship_dt < 19000101
-- OR sls_order_dt > 20500101 OR sls_order_dt < 19000101


  -- 4. order date must always be earlier than ship date or due date
  -- expect no result
  SELECT
  *
  FROM silver.crm_sales_details
  WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

  -- 5. SUM of sales must EQUAL to quantity * price
  -- negatives, zeroes or nulls are NOT ALLOWED
  -- expect no result
  SELECT
  sls_sales,
  sls_quantity,
  sls_price
  FROM silver.crm_sales_details
  WHERE sls_sales != sls_quantity * sls_price
  OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
  OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
  ORDER BY sls_sales, sls_quantity, sls_price

--============================================== silver.erp_cust_az12 data quality checks
--- 1. find any leftover NAS-- keys
SELECT 
cid,

CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) 
	ELSE cid
END cid,
bdate,
gen
FROM silver.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) 
	ELSE cid
END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info)


-- 2. Identify Out-of-Range dates
-- expect empty, or bad data
SELECT DISTINCT
bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

-- 3. find all possible values for gender to standardize
SELECT DISTINCT 
gen,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	ELSE 'n/a'
	END AS gen
FROM silver.erp_cust_az12

--============================================== silver.erp_loc_a101 data quality checks
-- 1. remove minus from cid to join tables
SELECT
cid
FROM silver.erp_cust_az12
WHERE REPLACE(cid,'-','') NOT IN (SELECT cst_key FROM silver.crm_cust_info)
/*
checks if any minuses were left back
append to top query
WHERE REPLACE(cid,'-','') NOT IN (SELECT cst_key FROM silver.crm_cust_info)
*/

-- check if country naming schemes are consistent
SELECT DISTINCT 
cntry
FROM silver.erp_loc_a101
ORDER BY CNTRY

--============================================== silver.erp_px_cat_g1v2 data quality checks
-- 1. Check for unwanted spaces
SELECT * FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) or subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

-- 2. Data Standardization & Consistency
-- query each row individually
SELECT DISTINCT
--cat
--subcat
maintenance
FROM silver.erp_px_cat_g1v2
