/*
-- save frequently used sql codein stored procedured in database - day to day activity

Stored Procedure : load Bronze layer (Source -> Bronze)

Script purpose :
    This stored procedure loads data into bronze schema from external csv files.
    It performs the following actions:
        truncates the bronze tables before loading data.
        uses the bulk insert command to load data from csv files to bronze tables

parameters:
    None
    This stored procedure does not accept any parameters or return any values

usage example:
     Exec bronze.load_bronze

*/

EXEC bronze.load_bronze -- to execute in simple way

DROP PROCEDURE IF EXISTS bronze.load_bronze;
GO

CREATE or ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
    BEGIN TRY
        set @batch_start_time = GETDATE()
        print '-----------------------------------------------------------------------------------------';
        print 'Load Bronze Layer';
        print '-----------------------------------------------------------------------------------------';
        print 'Load CRM Tables';
        print '-----------------------------------------------------------------------------------------';

        set @start_time = GETDATE()
        print '>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE table bronze.crm_cust_info; -- making table empty and then we do bulk insert

        print '>> Inserting Data Into: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        from '/var/opt/mssql/cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        set @end_time = GETDATE()
        print '>> Load Duration: '+ cast(DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR) + 'seconds';
        print '-----------------------------------------------------------------------------------------';

        set @start_time = GETDATE()
        print '>> Truncating Table: bronze.crm_prd_info';
        TRUNCATE table bronze.crm_prd_info; -- making table empty and then we do bulk insert

        print '>> Inserting Data Into: bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        from '/var/opt/mssql/prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        set @end_time = GETDATE()
        print '>> Load Duration: '+ cast(DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR) + 'seconds';
        print '-----------------------------------------------------------------------------------------';

        set @start_time = GETDATE()
        print '>> Truncating Table: bronze.crm_sales_details';
        TRUNCATE table bronze.crm_sales_details; -- making table empty and then we do bulk insert

        print '>> Inserting Data Into: bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        from '/var/opt/mssql/sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        set @end_time = GETDATE()
        print '>> Load Duration: '+ cast(DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR) + 'seconds';
        print '-----------------------------------------------------------------------------------------';

        print '-----------------------------------------------------------------------------------------';
        print 'Load CRM Tables';
        print '-----------------------------------------------------------------------------------------';

        set @start_time = GETDATE()
        print '>> Truncating Table: bronze.erp_cust_az12';
        TRUNCATE table bronze.erp_cust_az12; -- making table empty and then we do bulk insert

        print '>> Inserting Data Into: bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        from '/var/opt/mssql/CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        set @end_time = GETDATE()
        print '>> Load Duration: '+ cast(DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR) + 'seconds';
        print '-----------------------------------------------------------------------------------------';

        set @start_time = GETDATE()
        print '>> Truncating Table: bronze.erp_loc_a101';
        TRUNCATE table bronze.erp_loc_a101; -- making table empty and then we do bulk insert

        print '>> Inserting Data Into: bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        from '/var/opt/mssql/LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        set @end_time = GETDATE()
        print '>> Load Duration: '+ cast(DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR) + 'seconds';
        print '-----------------------------------------------------------------------------------------';

        set @start_time = GETDATE()
        print '>> Truncating Table: bronze.erp_px_cat_g1v2';
        TRUNCATE table bronze.erp_px_cat_g1v2; -- making table empty and then we do bulk insert

        print '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        from '/var/opt/mssql/PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        set @end_time = GETDATE()
        print '>> Load Duration: '+ cast(DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR) + 'seconds';
        print '-----------------------------------------------------------------------------------------';
        
        set @batch_end_time = GETDATE();
        print 'Loading Bronze layer is completed';
        print '>> Load Duration of whole broze layer batch: '+ cast(DATEDIFF(SECOND,@batch_start_time,@batch_end_time) as NVARCHAR) + 'seconds';
        print '-----------------------------------------------------------------------------------------';
    END TRY
    BEGIN CATCH
        print '-----------------------------------------------------------------------------------------';
        PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
        PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
        PRINT 'ERROR MESSAGE' + cast(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'ERROR MESSAGE' + cast(ERROR_STATE() AS NVARCHAR);
        print '-----------------------------------------------------------------------------------------';
    END CATCH
END
