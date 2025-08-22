


/*
===============================================================================================================================================
QUALITY CHECKS
===============================================================================================================================================
SCRIPT PURPOSE
		This script perform various quality checks for data consistency, accuracy and standardization across the silver schemas. it includes
		checks for:
		- Null or Duplicate primary keys
		- Unwanted spaces in strig fields
		-Data Standardization and consistency.
		-Invalid date ranges and orders.
		- Data consistency between related fields.


USAGE NOTES
		- Run these checks after data loading silver layer.
		-Investigate and resolve any discrepancies found during the checks
===============================================================================================================================================

*/


---------- CHECKING, CLEANING AND FIXING ISSUES IN crm_cust_info
SELECT * FROM bronze.crm_cust_info

----To check for duplicates and NULL
SELECT cst_id, COUNT(*) FROM silver.crm_cust_info
GROUP BY (cst_id)
HAVING count(cst_id) > 1 


----TO FIX DUPLICATES, WE USE Windows Function ROW()
SELECT * FROM bronze.crm_cust_info
WHERE cst_id = 29466


SELECT * FROM
(
	SELECT *,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS ranking
	FROM bronze.crm_cust_info
	) t
	WHERE ranking = 1

-------------TO CHECK FOR UNWANTED SPACES IN STRING VALES (QUALITY CHECK)
----- TO CHECK UNWANTED SPACES IN FRISTNAME TABLE
----- EXPECTATION: NO RESULT
SELECT cst_firstname FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

--------TO FIX UWANTED SPACES IN FIRSTNAME TABLE
SELECT TRIM(cst_firstname) AS cst_firstname
FROM bronze.crm_cust_info

 ----TO CHECK UNWANTED SPACES IN LASTTNAME TABLE
 -----EXPECTATION: NO RESULT
SELECT cst_lastname FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

--------TO FIX UWANTED SPACES IN LASTNAME TABLE
SELECT TRIM(cst_lastname) AS cst_lastname
FROM bronze.crm_cust_info


---------TO FIX DATA STANDARDIZATION ISSUES AND COSNTITENCY
SELECT DISTINCT(cst_gndr) to get distinct value
-FROM bronze.crm_cust_info
---------We need to replace Null with N/A, F with Female and M with Male USING CASE WHEN
---------WE replace NULL with N/A, M with Married and S with Single usign CASE WHEN

---------QUALITY ASSURANCE-----
---------CHECK FOR UNWANTED SPACES---------
----------EXPECTATION: NO RESULTS---------
SELECT * FROM 
bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num)


----------CHECK FOR NULLS OR NEGATIVE NUMBERS
--------EXPECTATION: NO RESULTS
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0

-----------DATA STANDARDIZATION AND CONSISTENCY
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info


---------CHECK FOR INVALID DATE ORDERS-------------
----EXPECTATION: END DATES CAN NOT BE LESSER THAN OR EARLIER THAN START DATES AND VICE VERSA
SELECT * 
FROM silver.crm_prd_info
WHERE prd_start_dt > prd_end_dt

-------------TESTING AND CHECKING FOR crm_prd_info
SELECT  
	prd_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt,
	LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509','AC-HE-HL-U509-B')


SELECT prd_id, COUNT(*) FROM silver.crm_prd_info
GROUP BY prd_id
HAVING count(prd_id) > 1 OR prd_id IS NULL;

-------------TESTING AND CHECKING FOR crm_sales_details

SELECT sls_ord_num,
	sls_prd_key,	
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,	
	sls_sales,
	sls_quantity,
	sls_price

FROM bronze.crm_sales_details
SELECT sls_ord_num,
	sls_prd_key,	
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,	
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details

WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)


SELECT * FROM bronze.erp_cust_az12

 SELECT * FROM bronze.erp_px_cat_g1v2
 SELECT * FROM bronze.erp_loc_a101



--------------- CHECK FOR INVALID DATES-----
/* FOR sls_order_dt, sls_due_dt, sls_ship_dt
checking for invalid dates include 
a. checking for dates that are 0 are replace them with NULL
b. Checking for the dates without the normal lengths of 8 characters
c. checking to make sure the dates are within the time range of the date

*/
SELECT 
NULLIF(sls_due_dt,0) sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 OR sls_order_dt > 20500101

----------------------d. order dates must always be earlier than shipping date or due date

SELECT * FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

/*  BUSINESS RULE
	---- SALES = Quantity * Price
	------ NO Negative, Zero, Nulls are not allowed
*/
----CHECK FOR DATA CONSISTENCY BETWEEN: Sales, Quantity and Price
------> Sales =  Quantity * Price
------> Values must not be Negative, Zero or NULL
/*
Rulesto FIX the issues above
---- If Sales is Negative, Zero or NULL, derive it using Quantity and Price
----- If Price is Zero or NULL, calculate it using Sales and Quantity.
------If Price is Negative, Convert it to a Positive Value
*/

SELECT DISTINCT
sls_sales AS old_sls_sales,
	CASE WHEN sls_sales != sls_quantity * ABS(sls_price) OR sls_sales <= 0 OR sls_sales IS NULL 
			THEN sls_quantity * ABS (sls_price)
		ELSE sls_sales
	END AS sls_sales,
sls_quantity,
sls_price AS old_sls_price,
	CASE WHEN sls_price IS NULL OR sls_price <= 0 
			THEN sls_sales/ NULLIF (sls_quantity, 0)
		ELSE ABS(sls_price)
	END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price


-----QUALITY CHECK FOR SILVER TABLE
----- Check for Invalid Date Orders
SELECT
*
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

----PASSED AS THE QUERY RETURN BLANK TABLE

----- Check for NULL, ZERO OR NEGATIVE VALES IN SALES, QUANTITY AND PRICE
SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price

-----PASSED AS THE QUERY ABOVE RETURNED BLANK TABLE

SELECT * FROM silver.crm_sales_details


---------- CHECKING, CLEANING AND FIXING ISSUES IN erp_px_cat_g1v2
SELECT * FROM bronze.erp_cust_az12
SELECT * FROM silver.crm_cust_info

/*
--it is observed cid  in .erp_cust_azi2 table is similar to cst_key in .crm_cust_info and with proper transformation, they can be useful for JOIN

cid is not consistent like cst_keys. we proceed to fix that
--- it is observed that some dates are not realistic and some are future dates. this will be fix
--bdate is observed to contain future dates, that will be corrected with case when 
---gen table contains uniques values that are not consistent and lack standardisation. it should be female, male and N/A for gender not known, we will use case when to fix that........

*/
SELECT cid AS old_cid,
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
				ELSE cid
		END AS cid,
		bdate AS old_bdate,
		CASE WHEN bdate <= '1914-01-01' OR bdate > GETDATE() THEN NULL
		 ELSE bdate
		 END AS bdate,
		gen AS old_gen,
		CASE WHEN TRIM(UPPER(gen)) IN ('F' ,'FEMALE' ) THEN 'Female'  ----------------
			WHEN TRIM(UPPER(gen)) = 'M' OR gen = 'Male' THEN 'Male' ------------------ BOTH will give the same result, lets just say i like to show off
			ELSE 'N/A'
		END AS gen
FROM bronze.erp_cust_az12



---- TO VERIFY THAT EVERY cst_key in silver.crm_cust_info table ia in Bronze.erp_cust_az12----------
SELECT cid AS old_cid,
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
				ELSE cid
		END AS cid,
		bdate,
		gen 
FROM bronze.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
				ELSE cid
	END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info)

-------TO VERIFY THAT BDATE CONTAINS THE RIGHT DATE THAT IS NOT FUTURE DATE AND NOT TOO OLD (OUT OF RANGE DATES)----------
SELECT bdate
FROM bronze.erp_cust_az12
WHERE bdate <= '1924-01-01' ----------NOT DATE OLDER THAT 100 years

SELECT bdate
FROM bronze.erp_cust_az12
WHERE bdate > GETDATE ()  ----------------------16 FUTURE RECORDS detected

--------- DATA Standardization and Consisitency: TO CHECK gen TABLE To see the uniques values it comntains----------
SELECT DISTINCT
gen 
FROM bronze.erp_cust_az12  --------------------------contains 6 uniques values such as NULL and blanks, it should be male,female and N/A where necessary

---------QUALITY CHECK
SELECT DISTINCT
gen 
FROM silver.erp_cust_az12  -------------------------------- DATA Standardization and consistenct issue fixed


/* CHECKING, CLEANING ON  cid in bronze.erp_loc_a101 to make it align with details of cst_key in silver.crm_cust_info
-------NO NULL OR ZERO IS DETECTED IN cid table
--------- NULL DETECTED IN CNTRY

*/

SELECT * FROM silver.crm_cust_info
 SELECT * FROM bronze.erp_loc_a101

 SELECT cid,
 Cntry
 FROM bronze.erp_loc_a101



 SELECT cid AS old_cid,
 REPLACE (cid, '-', '') AS cid,
 Cntry AS old_Cntry,
 CASE WHEN UPPER(TRIM(Cntry)) IN ('US', 'USA', 'UNITED STATES') THEN 'United State'
  WHEN UPPER(TRIM(Cntry)) = 'DE' THEN 'Germany'
  WHEN UPPER(TRIM(Cntry)) IS NULL OR UPPER(TRIM(Cntry)) = '' THEN 'n/a'
  ELSE TRIM(Cntry)
  END AS Cntry
 FROM bronze.erp_loc_a101

--------REMOVAL OF THE UNNESSARY '-' IN THE cid column-----------
SELECT cid,
REPLACE (cid, '-', '') AS cid
FROM bronze.erp_loc_a101
WHERE REPLACE (cid, '-', '')  NOT IN (SELECT cst_key FROM silver.crm_cust_info) -------------NO UN MATCHING DATA IS FOUND


-------- CHECK  FOR DATA STANDARDIZATION AND CONSISTENCY
SELECT DISTINCT
Cntry AS old_cntry,
CASE WHEN UPPER(TRIM(Cntry)) IN ('US', 'USA', 'UNITED STATES') THEN 'United State'
  WHEN UPPER(TRIM(Cntry)) = 'DE' THEN 'Germany'
  WHEN UPPER(TRIM(Cntry)) IS NULL OR UPPER(TRIM(Cntry)) = '' THEN 'n/a'
  ELSE TRIM(Cntry)
  END AS Cntry
FROM bronze.erp_loc_a101


-----------------------------QUALITY CHECK OF THE silver.erp_loc_a101 TABLE----------
SELECT cntry ----------------------------------Consistency and data standardization is effected in the country column
FROM silver.erp_loc_a101
GROUP BY cntry

SELECT cid					------------------------'-' in the cid column is now fixed
FROM silver.erp_loc_a101


/*
CHECKING, CLEANING AND LOADING DATA FROM bronze.erp_px_cat_g1v2 INTO silver.erp_px_cat_g1v2
 - CHECK FOR DUPLICATES, NULL, DATA INCONSISTENCY
 - ALL LOOKS GOOD, NO ISSUES IS DETECTED
*/

SELECT 
id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2


----------CHECK IF THERE IS DUPLICATE IN THE id column --------------
SELECT COUNT(id)
FROM bronze.erp_px_cat_g1v2
GROUP BY id
HAVING count(id) > 1 -----------------------it returns empty: no duplicate found

-----------CHECK FOR NULL OR DATA INCONSISTENCY IN cat, subcat and maintenance colunm
SELECT DISTINCT cat
FROM bronze.erp_px_cat_g1v2

SELECT DISTINCT maintenance
FROM bronze.erp_px_cat_g1v2

------CHECK FOR EXCESS SPACES IN cat, subcat and maintenance column-----------
SELECT subcat FROM
bronze.erp_px_cat_g1v2
WHERE subcat != TRIM(subcat)




SELECT prd

SELECT * FROM silver.crm_prd_info

SELECT id
FROM bronze.erp_px_cat_g1v2
WHERE id NOT IN (SELECT cat_id FROM silver.crm_prd_info)
