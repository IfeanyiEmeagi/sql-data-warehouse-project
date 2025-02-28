/*
* Project: SQL Data Warehouse
* Purpose: Creates the gold layer views from the silver layer table. 
*/

-- 1. Customer Dimension - Consolidated view of customer information
CREATE VIEW gold.dim_customers AS
SELECT
    ROW_NUMBER() OVER(ORDER BY ci.cst_id ) AS customer_key,
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    -- Handle missing gender values with fallback logic
    CASE WHEN ci.cst_gender != 'n/a' THEN ci.cst_gender
         ELSE COALESCE(ca.gender, 'n/a')
    END AS gender,
    ci.cst_marital_status AS marital_status,
    la.country,
    ca.bdate AS birth_date,
    ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la ON ci.cst_key = la.cid;

-- 2. Product Dimension - Current product catalog with categories
CREATE VIEW gold.dim_products AS
SELECT
    -- Ordered by start date for consistent key generation
    ROW_NUMBER() OVER(ORDER BY pi.prd_start_date, pi.prd_id) AS product_key,
    pi.prd_id AS product_id,
    pi.prd_key AS product_number,
    pi.prd_name AS product_name,
    pi.cat_id AS category_id,
    pc.category,
    pc.sub_category,
    pi.prd_cost AS cost,
    pi.prd_line AS product_line,
    pc.maintenance,
    pi.prd_start_date AS start_date
FROM silver.crm_prod_info pi
LEFT JOIN silver.erp_px_cat_g1v2 pc ON pi.cat_id = pc.id
WHERE pi.prd_end_date IS NULL;  -- filters out all the historical products

-- 3. Sales Fact - Transactional data with dimensional references
CREATE VIEW gold.fact_sales AS
SELECT
    sls_order_num AS order_number,
    p.product_key,
    c.customer_key,
    sls_order_date AS order_date,
    sls_ship_date AS ship_date,
    sls_due_date AS due_date,
    sls_sales AS sales_amount,
    sls_quantity AS quantity,
    sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products p ON sd.sls_prd_key = p.product_number
LEFT JOIN gold.dim_customers c ON sd.sales_cust_id = c.customer_id;
