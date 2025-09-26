-- ================================================
-- Step1: Create Ticket Raw Table in Bronze Layer
-- ================================================
USE AirlinesDWH;
GO

IF OBJECT_ID('bronze.ticket', 'U') IS NOT NULL
    DROP TABLE bronze.ticket;
GO

CREATE TABLE bronze.ticket (
    ticket_id NVARCHAR(200) NULL,
    booking_class NVARCHAR(200) NULL,
    seat_number NVARCHAR(100) NULL,
    fare_type NVARCHAR(200) NULL,
    baggage_allowance NVARCHAR(100) NULL
);
GO

-- ================================================
-- Step 2: Clean Tickets in Silver Layer
-- ================================================

IF OBJECT_ID('silver.ticket', 'U') IS NOT NULL
    DROP TABLE silver.ticket;
GO

CREATE TABLE silver.ticket (
    ticket_id INT NOT NULL PRIMARY KEY,
    booking_class NVARCHAR(20) NULL,
    seat_number NVARCHAR(10) NULL,
    fare_type NVARCHAR(20) NULL,
    baggage_allowance NVARCHAR(100) NULL,
    ingestion_timestamp DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

-- ================================================
-- Step 3: Tickets Ingestion Stored Procedure
-- from Bronze to Silver
-- ================================================

CREATE OR ALTER PROCEDURE silver.load_ticket
AS
BEGIN
    DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY
        SET @batch_start_time = GETDATE();

        -- Step 1: Truncate Bronze
        TRUNCATE TABLE bronze.ticket;

        -- Step 2: Bulk Insert into Bronze
        BULK INSERT bronze.ticket
        FROM 'D:\Microsoft Data Engineer\Final Project\Medallion\BRONZE\ticket.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0a',
            TABLOCK
        );

        SELECT TOP 10 * FROM bronze.ticket;

        -- Step 3: Insert Clean Data into Silver
        TRUNCATE TABLE silver.ticket;

        INSERT INTO silver.ticket
        (
            ticket_id, booking_class, seat_number, fare_type, baggage_allowance
        )
        SELECT
            TRY_CAST(ticket_id AS INT) AS ticket_id,
            CASE 
                WHEN LOWER(booking_class) IN ('economy','business','first') 
                    THEN UPPER(booking_class)
                ELSE 'ECONOMY'
            END AS booking_class,
            NULLIF(seat_number, '') AS seat_number,
            CASE 
                WHEN LOWER(fare_type) IN ('refundable','non-refundable','promo') 
                    THEN UPPER(fare_type)
                ELSE 'Non-Refundable'
            END AS fare_type,
            NULLIF(baggage_allowance, '') AS baggage_allowance
        FROM bronze.ticket
        WHERE TRY_CAST(ticket_id AS INT) IS NOT NULL;

        SELECT TOP 10 * FROM silver.ticket;

        SET @batch_end_time = GETDATE();
        PRINT('Load duration (seconds): ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS VARCHAR));
    END TRY
    BEGIN CATCH
        PRINT('Error occurred during Ticket loading');
        PRINT(ERROR_MESSAGE());
    END CATCH
END
GO

-- Run ingestion
EXEC silver.load_ticket;