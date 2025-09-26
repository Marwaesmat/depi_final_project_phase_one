-- ============================================
-- Dimension Tables
-- ============================================

-- Passenger Dimension
IF OBJECT_ID('gold.dim_passenger', 'U') IS NOT NULL
    DROP TABLE gold.dim_passenger;
GO
CREATE TABLE gold.dim_passenger (
    passenger_key INT IDENTITY(1,1) PRIMARY KEY,
    passenger_id INT NOT NULL,            -- from silver.passenger
    first_name NVARCHAR(100),
    last_name NVARCHAR(100),
    gender NVARCHAR(20),
    age INT,
    nationality NVARCHAR(50),
    loyalty_status NVARCHAR(20),
    has_complaints BIT,
    effective_date DATETIME2 NOT NULL DEFAULT getdate()
);


IF OBJECT_ID('gold.dim_flight', 'U') IS NOT NULL
    DROP TABLE gold.dim_flight;
GO
-- Flight Dimension
CREATE TABLE gold.dim_flight (
    flight_key INT IDENTITY(1,1) PRIMARY KEY,
    flight_id INT NOT NULL,               -- from silver.flight
    flight_number NVARCHAR(50),
    origin NVARCHAR(100),
    destination NVARCHAR(100),
    departure_time DATETIME,
    arrival_time DATETIME,
    aircraft_type NVARCHAR(50),
    effective_date DATETIME2 NOT NULL DEFAULT getdate()
);


IF OBJECT_ID('gold.dim_ticket', 'U') IS NOT NULL
    DROP TABLE gold.dim_ticket;
GO
-- Ticket Dimension
CREATE TABLE gold.dim_ticket (
    ticket_key INT IDENTITY(1,1) PRIMARY KEY,
    ticket_id INT NOT NULL,               -- from silver.ticket
    booking_class NVARCHAR(50),
    seat_number NVARCHAR(20),
    fare_type NVARCHAR(50),
    baggage_allowance NVARCHAR(20),       -- e.g. "25kg"
    effective_date DATETIME2 NOT NULL DEFAULT getdate()
);


IF OBJECT_ID('gold.dim_date', 'U') IS NOT NULL
    DROP TABLE gold.dim_date;
GO
-- Date Dimension (for reporting)
CREATE TABLE gold.dim_date (
    date_key INT PRIMARY KEY,             -- YYYYMMDD
    full_date DATE NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    weekday_name NVARCHAR(20)
);

-- ============================================
-- Fact Table
-- ============================================

IF OBJECT_ID('gold.fact_booking', 'U') IS NOT NULL
    DROP TABLE gold.fact_booking;
GO
CREATE TABLE gold.fact_booking (
    booking_id INT IDENTITY(1,1) PRIMARY KEY,
    
    -- Foreign Keys
    passenger_key INT NOT NULL,
    flight_key INT NOT NULL,
    ticket_key INT NOT NULL,
    booking_date_key INT NOT NULL,        -- FK to dim_date
    flight_date_key INT NOT NULL,         -- FK to dim_date
    
    -- Measures / Facts
    fare DECIMAL(10,2) NULL,
    has_complaints BIT NULL,

    -- Load audit
    ingestion_timestamp DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),

    -- Relationships
    FOREIGN KEY (passenger_key) REFERENCES gold.dim_passenger(passenger_key),
    FOREIGN KEY (flight_key) REFERENCES gold.dim_flight(flight_key),
    FOREIGN KEY (ticket_key) REFERENCES gold.dim_ticket(ticket_key),
    FOREIGN KEY (booking_date_key) REFERENCES gold.dim_date(date_key),
    FOREIGN KEY (flight_date_key) REFERENCES gold.dim_date(date_key)
);
