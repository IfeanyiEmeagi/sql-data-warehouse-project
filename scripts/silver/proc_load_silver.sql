
/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'silver' schema from bronze layer. 
    It performs the following actions:
    - Truncates the silver tables before loading data.
	- Transforms the data by performing data cleaning, standardization, quality check and data integrity.
    - Uses the `INSERT INTO` command to load data from bronze tables after transformation to silver tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME2, @end_time DATETIME2, @total_start_time DATETIME2, @total_end_time DATETIME2
	BEGIN TRY
	SET @total_start_time = GETDATE()
		PRINT '==========================================='
		PRINT '				Load Silver Layer			  '
		PRINT '==========================================='
		PRINT ''
		PRINT '------------------------------------------'
		PRINT '	Transformed CRM Loading...				 '
		PRINT '------------------------------------------'
		PRINT ''
		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: silver.crm_cust_info'
		TRUNCATE TABLE silver.crm_cust_info

		PRINT '>> Inserting data into: silver.crm_cust_info'
		INSERT INTO silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gender,
			cst_create_date
		)
		SELECT
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE 
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			ELSE 'n/a'
			END AS cst_marital_status,
			CASE 
				WHEN UPPER(TRIM(cst_gender)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'Male'
			ELSE 'n/a'
			END AS cst_gender,
			cst_create_date
		FROM 
		(SELECT
			*,
			ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
		FROM bronze.crm_cust_info)t
		WHERE flag_last = 1 AND cst_id IS NOT NULL;
		SET @end_time = GETDATE()
		
		PRINT '>> Transformation & Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.'
		PRINT ''
		PRINT '>>------------'
		PRINT ''

		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: silver.crm_prod_info'
		TRUNCATE TABLE silver.crm_prod_info

		PRINT '>> Inserting data into: silver.crm_prod_info'
		INSERT INTO silver.crm_prod_info(
			prd_id,
			cat_id,
			prd_key,
			prd_name,
			prd_cost,
			prd_line,
			prd_start_date,
			prd_end_date
		)

		SELECT
			prd_id,
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,  --Extract the product category id
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,			    --Extract the product subcategory id
			prd_name,
			ISNULL(prd_cost, 0) AS prd_cost,
			CASE UPPER(TRIM(prd_line))
				WHEN 'M' THEN 'Medium'
				WHEN 'S' THEN 'Small'
				WHEN 'T' THEN 'Top'
				WHEN 'R' THEN 'Revised'
				ELSE 'n/a'
			END AS prd_line,
			prd_start_date,
			DATEADD(day, -1, LEAD(prd_start_date) OVER(PARTITION BY prd_key ORDER BY prd_start_date)) AS prd_end_date
		FROM bronze.crm_prod_info
		SET @end_time = GETDATE()
		PRINT '>> Transformation & Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.'
		PRINT ''
		PRINT '>>------------'
		PRINT ''

		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: silver.crm_sales_details'
		TRUNCATE TABLE silver.crm_sales_details

		PRINT '>> Inserting data into: silver.crm_sales_details'
		INSERT INTO silver.crm_sales_details (
			sls_order_num,
			sls_prd_key,
			sales_cust_id,
			sls_order_date,
			sls_ship_date,
			sls_due_date,
			sls_sales,
			sls_quantity,
			sls_price
			)
		SELECT
			sls_order_num,
			sls_prd_key,
			sales_cust_id,
			CASE 
				WHEN sls_order_date <=0 OR LEN(sls_order_date) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_date AS VARCHAR) AS DATE)
			END AS sls_order_date,
			CASE 
				WHEN sls_ship_date <=0 OR LEN(sls_ship_date) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_date AS VARCHAR) AS DATE)
			END AS sls_ship_date,
			CASE 
				WHEN sls_due_date <=0 OR LEN(sls_due_date) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_date AS VARCHAR) AS DATE)
			END AS sls_due_date,
			CASE WHEN sls_sales IS NULL OR sls_sales != sls_quantity * sls_price OR sls_sales <= 0 
					THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales,
			sls_quantity,
			CASE WHEN sls_price IS NULL OR sls_price <= 0 
					THEN sls_sales / NULLIF(sls_quantity, 0)
				ELSE sls_price
			END AS sls_price
		FROM bronze.crm_sales_details
		SET @end_time = GETDATE()
		PRINT '>> Transformation & Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.'
		PRINT ''

		PRINT '------------------------------------------';
		PRINT '	Transformed ERP Loading...			     ';
		PRINT '------------------------------------------';

		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: silver.erp_cust_az12'
		TRUNCATE TABLE silver.erp_cust_az12

		PRINT '>> Inserting data into: silver.erp_cust_az12'

		INSERT INTO silver.erp_cust_az12 (cid, bdate, gender)
		SELECT
			CASE 
				WHEN cid LIKE 'NAS%' THEN TRIM(SUBSTRING(cid, 4, LEN(cid))) --Extract the customer to
				ELSE cid
			END AS cid,
			CASE WHEN bdate < '1900-01-01' OR bdate > GETDATE() THEN NULL -- Set future birthdate to NULL
				ELSE bdate
			END AS bdate,
			CASE 
        WHEN UPPER(TRIM(gender)) LIKE '%F%' OR UPPER(TRIM(gender)) LIKE '%FEMALE%' THEN 'Female'
        WHEN UPPER(TRIM(gender)) LIKE '%M%' OR UPPER(TRIM(gender)) LIKE '%MALE%' THEN 'Male'
        ELSE 'n/a'
    END AS gender -- Normalize gender values and handle unknown cases
		FROM bronze.erp_cust_az12
		SET @end_time = GETDATE()
		PRINT '>> Transformation & Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.'
		PRINT ''
		PRINT '>>------------'
		PRINT ''

		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: silver.erp_loc_a101'
		TRUNCATE TABLE silver.erp_loc_a101

		PRINT '>> Inserting data into: silver.erp_loc_a101'
		INSERT INTO silver.erp_loc_a101 (cid, country)
		SELECT 
			TRIM(REPLACE(cid, '-', '')) AS cid,
			CASE WHEN country IS NULL OR TRIM(country) = '' THEN 'n/a' 
				WHEN  UPPER(TRIM(country)) LIKE '%DE%' OR UPPER(TRIM(country)) LIKE '%GERMANY%' THEN 'Germany'
				WHEN UPPER(TRIM(country)) LIKE '%US%' OR UPPER(TRIM(country)) LIKE '%USA%' OR UPPER(TRIM(country)) LIKE '%UNITED STATES%' THEN 'United States'
				ELSE TRIM(country)
			END AS country
		FROM bronze.erp_loc_a101
		SET @end_time = GETDATE()
		PRINT '>> Transformation & Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.'
		PRINT ''
		PRINT '>>------------'
		PRINT ''

		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2'
		TRUNCATE TABLE silver.erp_px_cat_g1v2

		PRINT '>> Inserting data into: silver.erp_px_cat_g1v2'
		INSERT INTO silver.erp_px_cat_g1v2 (
		id, category, sub_category, maintenance
		)
		SELECT
			id,
			category,
			sub_category,
			maintenance
		FROM bronze.erp_px_cat_g1v2
		SET @end_time = GETDATE()
		PRINT '>> Transformation & Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.'
		PRINT ''
		PRINT '>>------------'
		PRINT ''
		SET @total_end_time = GETDATE()
		PRINT '>> Total Transformation and Load Duration: ' + CAST(DATEDIFF(SECOND, @total_start_time, @total_end_time) AS NVARCHAR) + ' seconds.'
		PRINT ''
	END TRY
		BEGIN CATCH
		PRINT '==================================================';
		PRINT '';
		PRINT 'An Error Occurred While Loading the Silver Layer'
		PRINT 'Error Message ' + ERROR_MESSAGE();
		PRINT 'Error Message ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT ' Error Message ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '';
		PRINT '==================================================';
		END CATCH
END

