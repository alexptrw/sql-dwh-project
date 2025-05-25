/*
=======================================================================
Quality Checks
=======================================================================
Sciprt Purpose:
  Example of some of the actions performed to check data  inconsistency, accuracy and Standardization  befor loading to silver schema:
  Checks made:
  - Null or duplicate PK
  - Unwanted leading or trailing spaces in str fields
  - Data standardization  and consistency
  - Invalid ranges for int and date/datetime fields
  - Data consistency between related fields
=======================================================================
*/

SELECT 
cst_firstname
  FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname) 

SELECT 
cst_lastname
  FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

SELECT 
prd_id,
COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL 

SELECT prd_id
      ,prd_key,
	  REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	  SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key
      ,prd_nm
      ,prd_cost
      ,prd_line
      ,prd_start_dt
      ,prd_start_dt
  FROM DataWarehouse.bronze.crm_prd_info 

  SELECT prd_nm FROM DataWarehouse.bronze.crm_prd_info
  WHERE TRIM(prd_nm) != prd_nm 

SELECT prd_cost FROM DataWarehouse.bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL 


SELECT * FROM  silver.crm_prd_info
WHERE prd_end_dt <prd_start_dt 


SELECT DISTINCT prd_line FROM  silver.crm_prd_info

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
FROM bronze.crm_sales_details
--WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)
--WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info)
--WHERE sls_ord_num != TRIM(sls_ord_num)

SELECT 
NULLIF(sls_order_dt, 0)
FROM bronze.crm_sales_details
WHERE sls_order_dt <=0 OR LEN(sls_order_dt) != 8 OR
sls_order_dt > 20500101 OR sls_order_dt < 1900101


SELECT 
NULLIF(sls_ship_dt, 0)
FROM bronze.crm_sales_details
WHERE sls_ship_dt <=0 OR LEN(sls_ship_dt) != 8 OR
sls_ship_dt > 20500101 OR sls_ship_dt < 1900101


SELECT 
NULLIF(sls_due_dt, 0)
FROM bronze.crm_sales_details
WHERE sls_due_dt <=0 OR LEN(sls_due_dt) != 8 OR
sls_due_dt > 20500101 OR sls_due_dt < 1900101

SELECT * FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR 
sls_order_dt > sls_due_dt

SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR
sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR 
sls_price <= 0
ORDER BY sls_quantity, sls_price

SELECT DISTINCT
CASE WHEN sls_sales IS NULL 
			OR sls_sales <= 0 
			OR sls_sales != sls_quantity * ABS(sls_price)
			THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
		END	sls_sales,
sls_quantity,
CASE WHEN sls_price IS NULL OR sls_price <=0
			THEN sls_sales/NULLIF(sls_quantity, 0)
	ELSE sls_price
	END AS sls_price
FROM bronze.crm_sales_details 

SELECT 
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
	ELSE cid
	END AS cid,
CASE WHEN bdate > GETDATE() THEN NULL
	ELSE bdate
	END as bdate,
CASE 
		WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		ELSE 'n/a' 
		END AS gen
FROM bronze.erp_cust_az12

SELECT bdate FROM bronze.erp_cust_az12
WHERE bdate > GETDATE()


SELECT CASE UPPER(TRIM(gen))
		WHEN 'F' THEN 'Female'
		WHEN 'M' THEN 'Male'
		WHEN '' THEN NULL
		ELSE gen
		END as gen
FROM bronze.erp_cust_az12

SELECT CASE UPPER(TRIM(gen))
		WHEN 'F' THEN 'Female'
		WHEN 'M' THEN 'Male'
		WHEN '' THEN NULL
		ELSE gen
		END as gen
FROM bronze.erp_cust_az12

SELECT  *
FROM silver.erp_cust_az12


SELECT bdate FROM silver.erp_cust_az12
WHERE bdate > GETDATE() 

SELECT 
REPLACE(cid, '-', '') as cid, 
CASE 
		WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN('US', 'USA') THEN 'United States'
		WHEN TRIM(cntry) = ''  OR TRIM(cntry) IS NULL THEN 'n/a'
		ELSE TRIM(cntry)
		END AS cntry FROM bronze.erp_loc_a101


SELECT CASE 
		WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN('US', 'USA') THEN 'United States'
		WHEN TRIM(cntry) = ''  OR TRIM(cntry) IS NULL THEN 'n/a'
		ELSE TRIM(cntry)
		END AS cntry
FROM bronze.erp_loc_a101

SELECT DISTINCT cntry FROM silver.erp_loc_a101 

SELECT id, cat, subcat, maintenance FROM bronze.erp_px_cat_g1v1

SELECT maintenance FROM bronze.erp_px_cat_g1v1
WHERE maintenance != TRIM(maintenance)


SELECT DISTINCT maintenance FROM bronze.erp_px_cat_g1v1

SELECT id, cat, subcat, maintenance FROM silver.erp_px_cat_g1v1
