-- ================================================
-- Step1: Create Flight Raw Table in Bronze Layer
-- ================================================
USE AirlinesDWH;
GO

IF OBJECT_ID('bronze.flight', 'U') IS NOT NULL
    DROP TABLE bronze.flight;
GO

CREATE TABLE bronze.flight (
    flight_id NVARCHAR(200) NULL,
    flight_number NVARCHAR(200) NULL,
    carrier_name NVARCHAR(400) NULL,
    carrier_code NVARCHAR(50) NULL,
    aircraft_type NVARCHAR(200) NULL,
    origin_airport_code NVARCHAR(50) NULL,
    destination_airport_code NVARCHAR(50) NULL,
    scheduled_departure_time NVARCHAR(200) NULL,
    actual_departure_time NVARCHAR(200) NULL,
    scheduled_arrival_time NVARCHAR(200) NULL,
    actual_arrival_time NVARCHAR(200) NULL,
    flight_status NVARCHAR(100) NULL
);
GO


-- ================================================
-- Step 2: Clean Flights in Silver Layer
-- ================================================

IF OBJECT_ID('silver.flight', 'U') IS NOT NULL
    DROP TABLE silver.flight;
GO

CREATE TABLE silver.flight (
    flight_id INT NOT NULL PRIMARY KEY,
    flight_number NVARCHAR(50) NOT NULL,
    carrier_name NVARCHAR(200) NULL,
    carrier_code NVARCHAR(20) NULL,
    aircraft_type NVARCHAR(100) NULL,
    origin_airport_code NVARCHAR(10) NULL,
    destination_airport_code NVARCHAR(10) NULL,
    scheduled_departure_time DATETIME NULL,
    actual_departure_time DATETIME NULL,
    scheduled_arrival_time DATETIME NULL,
    actual_arrival_time DATETIME NULL,
    flight_status NVARCHAR(20) NOT NULL,
    ingestion_timestamp DATETIME2 NOT NULL DEFAULT getdate()
);
GO

-- ================================================
-- Step 3: Flights Ingestion Stored Procedure
-- from Bronze to Silver
-- ================================================

CREATE OR ALTER PROCEDURE silver.load_flight
AS
BEGIN
    DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY
        SET @batch_start_time = GETDATE();

        -- Step 1: Truncate Bronze
        TRUNCATE TABLE bronze.flight;

        -- Step 2: Bulk Insert into Bronze
        BULK INSERT bronze.flight
        FROM 'D:\Microsoft Data Engineer\Final Project\Medallion\BRONZE\flight.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0a',
            TABLOCK
        );

        SELECT TOP 10 * FROM bronze.flight;

        -- Step 3: Insert Clean Data into Silver
        TRUNCATE TABLE silver.flight;

        INSERT INTO silver.flight
        (
            flight_id, flight_number, carrier_name, carrier_code, aircraft_type,
            origin_airport_code, destination_airport_code,
            scheduled_departure_time, actual_departure_time,
            scheduled_arrival_time, actual_arrival_time,
            flight_status
        )
        SELECT
            TRY_CAST(flight_id AS INT),
            NULLIF(flight_number, ''),
            NULLIF(carrier_name, ''),
            UPPER(NULLIF(carrier_code, '')),
            NULLIF(aircraft_type, ''),
            UPPER(NULLIF(origin_airport_code, '')),
            UPPER(NULLIF(destination_airport_code, '')),
            TRY_CAST(scheduled_departure_time AS DATETIME),
            TRY_CAST(actual_departure_time AS DATETIME),
            TRY_CAST(scheduled_arrival_time AS DATETIME),
            TRY_CAST(actual_arrival_time AS DATETIME),
            CASE 
                WHEN LOWER(flight_status) IN ('on-time','ontime','scheduled') THEN 'Scheduled'
                WHEN LOWER(flight_status) IN ('delayed','late') THEN 'Delayed'
                WHEN LOWER(flight_status) IN ('cancelled','canceled') THEN 'Cancelled'
				WHEN LOWER(flight_status) IN ('diverted', 'rerouted', 'disrupted') THEN 'Diverted'
                ELSE 'Unknown'
            END
        FROM bronze.flight
        WHERE TRY_CAST(flight_id AS INT) IS NOT NULL
          AND NULLIF(flight_number, '') IS NOT NULL;

        SELECT TOP 10 * FROM silver.flight;

        SET @batch_end_time = GETDATE();
        PRINT('Load duration (seconds): ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS VARCHAR));
    END TRY
    BEGIN CATCH
        PRINT('Error occurred during Flight loading');
        PRINT(ERROR_MESSAGE());
    END CATCH
END
GO

-- Run ingestion
EXEC silver.load_flight;