-- ================================================
-- Airlines Data Warehouse Database & Schemas
-- ================================================

IF DB_ID('AirlinesDWH') IS NULL
    CREATE DATABASE AirlinesDWH;
GO

USE AirlinesDWH;
GO

-- Create Schemas
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'bronze') EXEC('CREATE SCHEMA bronze');
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'silver') EXEC('CREATE SCHEMA silver');
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')   EXEC('CREATE SCHEMA gold');
GO
