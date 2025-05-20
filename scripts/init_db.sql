/*
Create DB and Schemas
______________________
Creating DB DataWarehouse if it does not exists + sets up 3 schemas for learning purposes.
______________________

*/

USE master;

IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'DataWarehouse')
BEGIN
    CREATE DATABASE DataWarehouse;
END;
GO

USE DataWarehouse;
GO

CREATE SCHEMA bronze;
GO 
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
