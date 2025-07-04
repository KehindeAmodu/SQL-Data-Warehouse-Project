/*
=====================================================================================================================================================
CREATE DATABASE AND SCHEMAS
=====================================================================================================================================================
Script Purpose: 
	This script creates a new database named 'DataWareHouse' after checking if it already exists. If the database exists, it is dropped and recreated. additionally, the script sets up threw
	schemas within the databse: 'bronze', 'silver' and 'gold'.

WARNING:
	Running this script will driop the entire 'DataWareHouse' database if it exits.
	All data in the databse will be permanently deleted. Proceed with caution and ensure you have proper backups before running this script.
*/


USE master;
GO
------Drop and recreate the dataWarehouse database

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWareHouse')
	BEGIN
		ALTER DATABASE DataWareHouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
		DROP DATABASE DataWareHouse;
	END;
GO


--------------CREATE the Database named DataWareHouse
CREATE DATABASE DataWareHouse;
GO

USE DataWareHouse;
GO

------Create schema for bronze , silver and Gold phases
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
