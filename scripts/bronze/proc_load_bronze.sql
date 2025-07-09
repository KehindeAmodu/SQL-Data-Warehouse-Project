/* Loading the Database Tables with informatioon from the CSV files  using BULK INSERT
CREATE A PROCEDURE for loading the CSV files into the Bronze layer database
TO RUN the procedure....EXEC bronze.load_bronze
PRINT to display details of the processes running
Track ETL Duration is needed as it helps to identify bottlenecks, optimize performance, monitor trneds, detect issues

*/
/*
========================================================================================================================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
========================================================================================================================================================================
Script Purpose:
 This stored procedure loads data into the  'Bronze' Schema from external CSV files.
 It perfomrs the following acrions:
 - Truncates the Bronze Tables before loading Data.
 -Uses the 'BULK INSERT' command to load data from CSV files to bronze tables

 Parameters:
 None
 This stored procedure does not accept any parameters or return any values.
 
 Usage Example:
 EXEC bronze.load_bronze;

========================================================================================================================================================================

*/




CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '==============================================================================================';
		PRINT 'LOADING BRONZE LAYER'
		PRINT '==============================================================================================';

		PRINT '-----------------------------------------------------------------------------------------------';
		PRINT 'Loading CRM Tables'
		PRINT '-----------------------------------------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info

		PRINT '>> Inseritng Data into: bronze.crm_cust_info ';
		BULK INSERT bronze.crm_cust_info
		FROM 'D:\DATA ANALYSIS PROJECT\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
				FIRSTROW =2,
				FIELDTERMINATOR = ',',
				TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>-----------------';



		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info

		PRINT '>> Inseritng Data into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'D:\DATA ANALYSIS PROJECT\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
				FIRSTROW =2,
				FIELDTERMINATOR = ',',
				TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>-----------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details

		PRINT '>> Inseritng Data into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'D:\DATA ANALYSIS PROJECT\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
				FIRSTROW =2,
				FIELDTERMINATOR = ',',
				TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>-----------------';


		PRINT '-----------------------------------------------------------------------------------------------';
		PRINT 'Loading ERP Tables'
		PRINT '-----------------------------------------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_azi2';
		TRUNCATE TABLE bronze.erp_cust_azi2

		PRINT '>> Inseritng Data into: bronze.erp_cust_azi2' ;
		BULK INSERT bronze.erp_cust_azi2
		FROM 'D:\DATA ANALYSIS PROJECT\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
				FIRSTROW =2,
				FIELDTERMINATOR = ',',
				TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>-----------------';



		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101'	;	
		TRUNCATE TABLE bronze.erp_loc_a101

		PRINT '>> Inserting Data into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'D:\DATA ANALYSIS PROJECT\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
				FIRSTROW =2,
				FIELDTERMINATOR = ',',
				TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>-----------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2

		PRINT '>> Inserting Data into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'D:\DATA ANALYSIS PROJECT\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
				FIRSTROW =2,
				FIELDTERMINATOR = ',',
				TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>-----------------';

		SET @batch_end_time = GETDATE();
		PRINT '=============================================================================='
		PRINT 'Loading Bronze Layer is Completed';
		PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=============================================================================='
	END TRY
	BEGIN CATCH
		 PRINT'============================================================================================='
		 PRINT'ERROR OCCURED DURING LOADING  BRONZE LAYER'
		 PRINT'ERROR MESSAGE' + ERROR_MESSAGE();
		 PRINT'ERROR MESSAGE' + CAST (ERROR_NUMBER() AS NVARCHAR);
		 PRINT'ERROR MESSAGE' + CAST (ERROR_STATE() AS NVARCHAR);
		 PRINT'============================================================================================='
	END CATCH
END
