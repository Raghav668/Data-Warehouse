
/*
DDL SCRIPT: Create Gold Views

Script purpose:
   This script creates views for the gold layer in the data warehouse.
   The Gold layer represents the final dimension and fact tables (start schema)

   Each view performs transformations and combines data from the silver layer
   to produce a clean, enriched and business ready dataset

usage :
   These views can be queried directly for analytics and reporting
*/

If OBJECT_ID('gold.dim_customers','V') is not NULL
   drop view gold.dim_customers;
go

CREATE VIEW gold.dim_customers as 
SELECT 
ROW_NUMBER()over(order by cst_id) as customer_key,
ci.cst_id as customer_id,
ci.cst_key as customer_number,
ci.cst_firstname as first_name,
ci.cst_lastname as last_name,
loc.cntry as country,
ci.cst_martial_status as marital_status,
case 
   when ci.cst_gndr != 'Unknown' then ci.cst_gndr
   else coalesce(ca.gen,'Unknown')
end as gender,
ca.bdate as birthdate,
ci.cst_create_date as create_date
from silver.crm_cust_info ci
LEFT join silver.erp_cust_az12 ca 
on ci.cst_key = ca.cid
LEFT join silver.erp_loc_a101 loc 
on ci.cst_key = loc.cid 

go 

If OBJECT_ID('gold.dim_products','V') is not NULL
   drop view gold.dim_products;
go

create view gold.dim_products as
SELECT
ROW_NUMBER()over(order by ci.prd_start_dt,ci.prd_key) as product_key, 
ci.prd_id as product_id,
ci.prd_key as product_number, 
ci.prd_nm as product_name,
ci.cat_id as category_id,
ca.cat as category,
ca.subcat as subcategory,
ca.maintenance, 
ci.prd_cost as cost,
ci.prd_line as product_line,
ci.prd_start_dt as start_date
from silver.crm_prd_info ci
left join silver.erp_px_cat_g1v2 ca
on ci.cat_id = ca.id
where ci.prd_end_dt is NULL 

GO

If OBJECT_ID('gold.fact_sales','V') is not NULL
   drop view gold.fact_sales;
go

CREATE view gold.fact_sales as 
SELECT 
sd.sls_ord_num as order_number,
pr.product_key,
dc.customer_key,
sd.sls_order_dt as order_date,
sd.sls_ship_dt as shipping_date,
sd.sls_due_dt as due_date,
sd.sls_sales as sales_amount,
sd.sls_quantity as quantity,
sd.sls_price as price
from silver.crm_sales_details sd
left join gold.dim_customers dc 
on sd.sls_cust_id = dc.customer_id
left join gold.dim_products pr on sd.sls_prd_key = pr.product_number
