-- ================================================
-- Step1: Create Passenger Raw Table in Bronze Layer
-- ================================================
USE AirlinesDWH;
GO

IF OBJECT_ID('bronze.passenger', 'U') IS NOT NULL
    DROP TABLE bronze.passenger;
GO

CREATE TABLE bronze.passenger (
    --raw_passenger_id INT IDENTITY(1,1) PRIMARY KEY,
    passenger_id NVARCHAR(200) NULL,
    first_name NVARCHAR(400) NULL,
    last_name NVARCHAR(400) NULL,
    gender NVARCHAR(100) NULL,
    age NVARCHAR(100) NULL,
    nationality NVARCHAR(200) NULL,
    loyalty_status NVARCHAR(200) NULL,
    has_complaints NVARCHAR(100) NULL,
);
GO


-- ================================================
-- Step 2: Clean Passenger in Silver Layer
-- ================================================

IF OBJECT_ID('silver.passenger', 'U') IS NOT NULL
    DROP TABLE silver.passenger;
GO

CREATE TABLE silver.passenger (
    passenger_id INT NOT NULL PRIMARY KEY,
    first_name NVARCHAR(100) NULL,
    last_name NVARCHAR(100) NULL,
    gender NVARCHAR(20) NULL,
    age INT NULL,
    nationality NVARCHAR(50) NULL,
    loyalty_status NVARCHAR(20) NULL,
    has_complaints BIT NOT NULL DEFAULT(0),
    ingestion_timestamp DATETIME2 NOT NULL DEFAULT getdate()
);
GO


-- ================================================
-- Step 3: Passenger Ingestion Stored Procedure
-- from Bronze to Silver
-- ================================================

create or alter procedure silver.load_passenger
as
begin
	declare @batch_start_time datetime, @batch_end_time datetime;
	begin try
		set @batch_start_time = GETDATE();

		-- Truncate Bronze to eliminate double insert
		truncate table bronze.passenger;

		-- Bulk Insert from CSV into Bronze passenger table
		BULK INSERT bronze.passenger
			FROM 'D:\Microsoft Data Engineer\Final Project\Medallion\BRONZE\passenger.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				ROWTERMINATOR = '0x0a',
				TABLOCK
			);
		select top 10 * from bronze.passenger;


		-- Step 2: Work on Silver layer
		TRUNCATE TABLE silver.passenger;

		INSERT INTO silver.passenger
			(passenger_id, first_name, last_name, gender, age, nationality, loyalty_status, has_complaints)
		SELECT
			TRY_CAST(passenger_id AS INT) AS passenger_id,
			NULLIF(first_name, '') AS first_name,
			NULLIF(last_name, '') AS last_name,
			CASE 
				WHEN LOWER(gender) IN ('m','male') THEN 'Male'
				WHEN LOWER(gender) IN ('f','female') THEN 'Female'
				ELSE 'Unknown'
			END AS gender,
			TRY_CAST(age AS INT) AS age,
			UPPER(NULLIF(nationality, '')) AS nationality,
			CASE 
				WHEN LOWER(loyalty_status) IN ('silver','gold','platinum') 
					 THEN UPPER(loyalty_status)
				ELSE 'None'
			END AS loyalty_status,
			CASE 
				WHEN LOWER(has_complaints) IN ('1','true','yes','y') THEN 1 
				ELSE 0 
			END AS has_complaints
		FROM bronze.passenger
		WHERE TRY_CAST(passenger_id AS INT) IS NOT NULL
		  AND NULLIF(first_name, '') IS NOT NULL
		  AND NULLIF(last_name, '') IS NOT NULL
		  AND TRY_CAST(age AS INT) BETWEEN 0 AND 120;

		select top 10 * from silver.passenger;

		set @batch_end_time = GETDATE();
		print('Load duration (seconds): ' + cast(datediff(second, @batch_start_time, @batch_end_time) as varchar));
	end try
	begin catch
		print('Error occurred during Passenger loading');
		print(error_message());
	end catch
end
GO

EXEC silver.load_passenger;



--For testing:
-- returns three columns: file_exists, file_is_directory, parent_directory_exists
--		EXEC master.dbo.xp_fileexist 'D:\Microsoft Data Engineer\Final Project\Medallion\BRONZE\passenger.csv';
--		SELECT servicename, service_account 
--		FROM sys.dm_server_services;