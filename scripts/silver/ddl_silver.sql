/*
========================================================================================================================================================
Script Purpose:
  The script creates the tables at the silver layer for each of the table from the bronze layer. It checks if the
  table exists and if exists, it drops the table and recreate it.

WARNING:
  Apply caution while running this script as it deletes both the table and its content. Ensure you have backups before proceeding to run this script.
=========================================================================================================================================================
*/
IF OBJECT_ID ('silver.crm_cust_info', 'U') IS NOT NULL
DROP TABLE silver.crm_cust_info
CREATE TABLE silver.crm_cust_info (
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gender NVARCHAR(50),
	cst_create_date DATE,
	dwh_meta_created_date DATETIME2 DEFAULT GETDATE(),
);
GO

----Bronze Prod_info Table
IF OBJECT_ID ('silver.crm_prod_info', 'U') IS NOT NULL
DROP TABLE silver.crm_prod_info
CREATE TABLE silver.crm_prod_info (
	prd_id INT,
	cat_id NVARCHAR(50),
	prd_key NVARCHAR(50),
	prd_name NVARCHAR(50),
	prd_cost DECIMAL,
	prd_line NVARCHAR(50),
	prd_start_date DATE,
	prd_end_date DATE,
	dwh_meta_created_date DATETIME2 DEFAULT GETDATE(),
);
GO

IF OBJECT_ID ('silver.crm_sales_details', 'U') IS NOT NULL
DROP TABLE silver.crm_sales_details
CREATE TABLE silver.crm_sales_details(
	sls_order_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sales_cust_id INT,
	sls_order_date DATE,
	sls_ship_date DATE,
	sls_due_date DATE,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
	dwh_meta_created_date DATETIME2 DEFAULT GETDATE(),
);
GO

IF OBJECT_ID ('silver.erp_cust_az12', 'U') IS NOT NULL
	DROP TABLE silver.erp_cust_az12
CREATE TABLE silver.erp_cust_az12(
	cid NVARCHAR(50),
	bdate DATE,
	gender NVARCHAR(50),
	dwh_meta_created_date DATETIME2 DEFAULT GETDATE(),
);
GO

IF OBJECT_ID ('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
	DROP TABLE silver.erp_px_cat_g1v2
CREATE TABLE silver.erp_px_cat_g1v2(
	id NVARCHAR(50),
	category NVARCHAR(50),
	sub_category NVARCHAR(50),
	maintenance NVARCHAR(50),
	dwh_meta_created_date DATETIME2 DEFAULT GETDATE(),
);
GO

IF OBJECT_ID ('silver.erp_loc_a101', 'U') IS NOT NULL
	DROP TABLE silver.erp_loc_a101
CREATE TABLE silver.erp_loc_a101(
	cid NVARCHAR(50),
	country NVARCHAR(50),
	dwh_meta_created_date DATETIME2 DEFAULT GETDATE(),
);
