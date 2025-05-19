/*
-- Store commonly used SQL routines as stored procedures for daily operations

Stored Procedure: Load Silver Layer (Bronze -> Silver)

Purpose:
    This stored procedure handles the ETL workflow to load data from the bronze schema into the silver schema.
    Key steps include:
        - Clearing existing data in silver tables before loading
        - Inserting cleaned and transformed records from bronze tables into silver tables

Parameters:
    None
    This procedure does not take any input parameters or return output values.

Example usage:
    EXEC silver.load_silver;
*/


EXEC silver.load_silver
GO
DROP PROCEDURE IF EXISTS silver.load_silver;
GO

create or ALTER PROCEDURE silver.load_silver as 
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
    BEGIN TRY
        set @batch_start_time = GETDATE()
        print '===========================================================';
        print 'Load Silver Layer';
        print '===========================================================';
        print '-----------------------------------------------------------------------------------------';
        print 'Load CRM Tables';
        print '-----------------------------------------------------------------------------------------';

        set @start_time = GETDATE()
        print '>> Truncating Table silver.crm_cust_info'
        TRUNCATE TABLE silver.crm_cust_info;
        print '>> Inserting Data into silver.crm_cust_info'

        INSERT into silver.crm_cust_info (cst_id,cst_key,cst_firstname,cst_lastname,cst_martial_status,cst_gndr,cst_create_date) 
        select cst_id,cst_key,TRIM(cst_firstname) as cst_firstname,TRIM(cst_lastname) as cst_lastname,
        case upper(TRIM(cst_martial_status))
            when 'S' then 'Single'
            when 'M' then 'Married'
            else 'Unknown'
        end as cst_martial_status,
        case upper(TRIM(cst_gndr))
            when 'F' then 'Female'
            when 'M' then 'Male'
            else 'Unknown'
        end as case_gndr,cst_create_date from(
        select *, ROW_NUMBER() OVER(partition by cst_id order by cst_create_date desc) as rn
        from bronze.crm_cust_info) t where rn =1 
        set @end_time = GETDATE()
        print '>> Load Duration: '+ cast(DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR) + 'seconds';
        print '-----------------------------------------------------------------------------------------';

        set @start_time = GETDATE()
        print '>> Truncating Table silver.crm_prd_info'
        TRUNCATE TABLE silver.crm_prd_info;
        print '>> Inserting Data into silver.crm_prd_info'

        INSERT into silver.crm_prd_info (prd_id,cat_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt)
        select prd_id,
        Replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
        SUBSTRING(prd_key,7,len (prd_key)) as prd_key,
        prd_nm,coalesce(prd_cost,0) as prd_cost ,
        case UPPER(TRIM(prd_line))
            when 'M' then 'Mountain'
            when 'R' then 'Road'
            when 'S' then 'Sales'
            when 'T' then 'Touring'
            else 'Unknown' 
        end as prd_line , cast(prd_start_dt as date) as prd_start_date,
        cast(LEAD(prd_start_dt) OVER(partition by prd_key order by prd_start_dt)-1 as date) as prd_end_dt_test
        from bronze.crm_prd_info
        set @end_time = GETDATE()
        print '>> Load Duration: '+ cast(DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR) + 'seconds';
        print '-----------------------------------------------------------------------------------------';

        set @start_time = GETDATE()
        print '>> Truncating Table silver.crm_sales_details'
        TRUNCATE TABLE silver.crm_sales_details;
        print '>> Inserting Data into silver.crm_sales_details'

        INSERT into silver.crm_sales_details(sls_ord_num,
            sls_prd_key,
            sls_cust_id ,
            sls_order_dt ,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price)
        (select sls_ord_num,sls_prd_key,sls_cust_id,
        case 
            when sls_order_dt = 0 or len(sls_order_dt) !=8 then null
            else CAST(cast(sls_order_dt as varchar) as date)
        end as sls_order_dt,
        case 
            when sls_ship_dt = 0 or len(sls_ship_dt) !=8 then null
            else CAST(cast(sls_ship_dt as varchar) as date)
        end as sls_ship_dt,
        case 
            when sls_due_dt = 0 or len(sls_due_dt) !=8 then null
            else CAST(cast(sls_due_dt as varchar) as date)
        end as sls_due_dt,
        case 
            when sls_sales <=0 or sls_sales is null or sls_sales != sls_quantity*ABS(sls_price) then sls_quantity *ABS(sls_price) 
            else sls_sales
        end as sls_sales ,sls_quantity,
        case 
            when sls_price is null or sls_price<=0 then sls_sales/Nullif(sls_quantity,0)
            else sls_price
        end as sls_price
        FROM bronze.crm_sales_details) 
        set @end_time = GETDATE()
        print '>> Load Duration: '+ cast(DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR) + 'seconds';
        print '-----------------------------------------------------------------------------------------';

        print '-----------------------------------------------------------------------------------------';
        print 'Load ERP Tables';
        print '-----------------------------------------------------------------------------------------';

        set @start_time = GETDATE()
        print '>> Truncating Table silver.erp_cust_az12'
        TRUNCATE TABLE silver.erp_cust_az12;
        print '>> Inserting Data into silver.erp_cust_az12'

        INSERT into silver.erp_cust_az12(cid,
            bdate,
            gen)
        SELECT 
            case 
                when cid like 'NAS%' then SUBSTRING(cid,4,len(cid))
                else cid
            end as cid ,
            case 
                when bdate < '1924-01-01' or bdate > GETDATE() then NULL
                else bdate
            end as bdate,
            CASE 
                WHEN UPPER(TRIM(REPLACE(REPLACE(REPLACE(gen, CHAR(13), ''), CHAR(10), ''), NCHAR(8592), ''))) IN ('F', 'FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(REPLACE(REPLACE(REPLACE(gen, CHAR(13), ''), CHAR(10), ''), NCHAR(8592), ''))) IN ('M', 'MALE') THEN 'Male'
                ELSE 'Unknown'
            END AS gen
        from bronze.erp_cust_az12
        set @end_time = GETDATE()
        print '>> Load Duration: '+ cast(DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR) + 'seconds';
        print '-----------------------------------------------------------------------------------------';

        set @start_time = GETDATE()
        print '>> Truncating Table silver.erp_loc_a101'
        TRUNCATE TABLE silver.erp_loc_a101;
        print '>> Inserting Data into silver.erp_loc_a101'

        INSERT into silver.erp_loc_a101(cid,cntry)
        select Replace(cid,'-','') as cid,
        case 
                when upper(trim(replace(replace(replace(cntry, '↵', ''), char(13), ''), char(10), ''))) = 'DE' then 'Germany'
                when upper(trim(replace(replace(replace(cntry, '↵', ''), char(13), ''), char(10), ''))) in ('US','USA','UNITED STATES') then 'United States'
                when cntry is null or trim(replace(replace(replace(cntry, '↵', ''), char(13), ''), char(10), '')) = '' then 'Unknown'
                else trim(replace(replace(replace(cntry, '↵', ''), char(13), ''), char(10), ''))
            end as cntry
        from bronze.erp_loc_a101 
        set @end_time = GETDATE()
        print '>> Load Duration: '+ cast(DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR) + 'seconds';
        print '-----------------------------------------------------------------------------------------';

        set @start_time = GETDATE()
        print '>> Truncating Table silver.erp_px_cat_g1v2'
        TRUNCATE TABLE silver.erp_px_cat_g1v2;
        print '>> Inserting Data into silver.erp_px_cat_g1v2'

        INSERT into silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
        SELECT id,cat,subcat,
        trim(replace(replace(replace(maintenance, '↵', ''), char(13), ''), char(10), '')) as maintenance
        from bronze.erp_px_cat_g1v2
        set @end_time = GETDATE()
        print '>> Load Duration: '+ cast(DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR) + 'seconds';
        print '-----------------------------------------------------------------------------------------';

        set @batch_end_time = GETDATE();
        print 'Loading Silver layer is completed';
        print '>> Load Duration of whole Silver layer batch: '+ cast(DATEDIFF(SECOND,@batch_start_time,@batch_end_time) as NVARCHAR) + 'seconds';
        print '-----------------------------------------------------------------------------------------';
    END TRY
    BEGIN catch
        print '-----------------------------------------------------------------------------------------';
        PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER';
        PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
        PRINT 'ERROR MESSAGE' + cast(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'ERROR MESSAGE' + cast(ERROR_STATE() AS NVARCHAR);
        print '-----------------------------------------------------------------------------------------';
    END CATCH
END

