/*
========================================================================================================================================

GOLD LAYER QUALITY CHECKS

========================================================================================================================================
Script Purpose:
			This script creates performs quality checks to validate the integrity, consistency, and accuracy of the Gold layer. These checke ensure
:
		-- Uniqueness of surrogate keys in dimension table
		-- referencial integrity between facts and dimension tables
		-- validation of relationships in the data model for analytical purposes.
			

Usage: 
		- Run these checks after data loading silver layer
		- Invesitgate and resolve any discrepancies found during the checks.
========================================================================================================================================
*/

--- ========================================================================================================================================
--- CHECKING 'gold.dim_customers'
--- ========================================================================================================================================



-----CHECK FOR DUPLICATES AND UNIQUENESS OF CUSTOMER KEY--------
-----CHECKING THE JOINED TABLES FOR DUPLICATES---------
SELECT cst_id, COUNT(*) FROM (
SELECT 
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	ci.cst_gndr,
	ci.cst_create_date,
	ca.bdate,
	ca.gen,
	la.cntry
FROM
silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
ON ci.cst_key = ca.cid	
LEFT JOIN silver.erp_loc_a101 AS la
ON ci.cst_key = la.cid
)q
GROUP BY cst_id	
HAVING COUNT(cst_id) >1      ----------------IF cst_id any is found to be >1, then there is a duplicate value in the join



-----INTEGRATE COLUMNS cst-gndr' with column 'gen' together
/* CONDITIONS FOR INTEGRATION OF THE COLUMNS
if the value in 'cst-gndr' and column 'gen' is the same, then retain the value. 
if the value is missing in one, pick from the other one.
if the value is different, pick the one from 'cst-gndr'
replace the value that is not available in both column as N/A
*/
SELECT new_gen FROM (
SELECT 
	ci.cst_id,
	ci.cst_gndr,
	CASE WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr
	ELSE COALESCE(ca.gen, 'N/A')
	END AS new_gen,
	ca.gen
FROM
silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
ON ci.cst_key = ca.cid	
LEFT JOIN silver.erp_loc_a101 AS la
ON ci.cst_key = la.cid
)t
GROUP BY new_gen
----TO CHECK IF THERE IS NO DUPLICATE-----



----TO CHECK THE QUALITY OF DATA, CHECK FOR DUPLICATES----
SELECT prd_id, COUNT(*) FROM (
SELECT
pn.prd_id,
pn.cat_id,
pn.prd_key,
pn.prd_nm,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL-------IF THE END DATE IS NULL, THEN IT IS A CURRENT INFORMATION SINCE
)b
GROUP BY prd_id
HAVING count(prd_id) > 1 ------SINCE THIS RETURNS NO VALUE, THEN THERE IS NO DUPLICATE

----------FACT CHECK: CHECK IF ALL DIMENSION TABLES CAN SUCCESSFULLY JOIN TO THE FACT TABLE----
----FOREIGN KEY INTEGRITY (DIMENSION)
SELECT * FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL ----------THIS QUERY SHOULD NOT RETURN ANY VALUE TO PASS THE INTEGRITY TEST

