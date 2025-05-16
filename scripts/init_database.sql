/*
Creating Data base and schemas

script purpose : 
    This script creates a new database named Data warehouse after checking if it already exist.
    If bthe database exists, it is dropped and recreated. Additionally, the script sets up three schemas
    withiin the database: 'bronze', 'silver, 'gold'.

Warning : 
    Running this script will drop the entire Data warwhouse database if it exists.
    All data in the database will be permanently deleted. proceed with caution
    and ensure you have proper backups before running the script.
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
