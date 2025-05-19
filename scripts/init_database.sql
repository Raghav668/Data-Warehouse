/*
Database and Schema Setup Script

Purpose:
    This script initializes a new database called 'Data Warehouse'. 
    It first checks for the existence of this database, and if found, it will drop and recreate it.
    Afterward, it creates three distinct schemas within the database: 'bronze', 'silver', and 'gold'.

Important Notice:
    Executing this script will delete the existing 'Data Warehouse' database along with all its data.
    Please make sure to back up any important information before proceeding.
    Use this script with caution to avoid unintended data loss.
*/

use master; 
GO

-- DROP AND RECREATE THE DATA WAREHOUSE DATABASE
IF EXISTS (SELECT 1 FROM sys.databases where name='DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

-- create DataWarehouse Database
create DATABASE DataWarehouse;
GO

use DataWarehouse;
GO

-- create schemas
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
