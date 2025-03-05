/*
	======================================================
		CREATE DATABASE AND SCHEMAS
	======================================================

Script Purpose:
	The script creates a new database named 'DataWarehouse', after checking if the database exists.
	If the database exists, it drops the database before creating a new one. Additionally, the scripts
	create three schemas, namely, 'bronze', 'silver' and 'gold' within the database.

WARNING:
	Running this script will drop the 'DataWarehouse' database if its exists. This will permanently delete all 
	files contained in the database. Proceed with caution and ensure you have backups before running this script.

*/

--- Switch to master database
USE master;
GO

---Check if the database exist, drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

---Create database DataWarehouse
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

---Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
