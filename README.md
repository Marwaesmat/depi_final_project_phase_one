# depi_final_project_phase_one
Airlines Data Warehouse Project (Phase One)

📌 Overview

This project demonstrates the design and implementation of a Data Warehouse for an Airlines company, built using Python, SQL Server, Talend, and Apache Airflow (Docker).
The solution follows a multi-layered architecture (Bronze → Silver → Gold) with automated orchestration.

🏗️ Architecture
1. Data Generation (Bronze Layer)

Tables created: passengers, flights, tickets, date

Method: Python + Pandas + Faker

Output: Raw CSV files

Purpose: Source data for the Bronze layer

2. Bronze Layer (Raw Data Storage)

Platform: SQL Server

Action: Load raw CSV files into SQL Server tables

Content: Unprocessed, original data

3. Silver Layer (Refined Data)

Action: Clean and refine Bronze data

Tables:

passengers_silver

flights_silver

tickets_silver

Purpose: Standardized, cleaned data free of duplicates and inconsistencies

4. Gold Layer (Dimensional Modeling)

Dimension Tables:

dim_passengers

dim_flights

dim_tickets

dim_date

🔹 Fact Table: fact_booking

Purpose: Central transactional table used for analysis and reporting

Built from: Dimension tables above

ETL Flow in Talend:

Extract primary keys from dim_passengers, dim_flights, dim_tickets, and dim_date

Join them with the refined transactional data (from Silver layer)

Generate foreign key relationships between fact and dimension tables

Load the result into the fact_booking table

Fact Booking Example Structure:

booking_id (primary key)

passenger_id (FK → dim_passengers)

flight_id (FK → dim_flights)

ticket_id (FK → dim_tickets)

date_id (FK → dim_date)

amount (measure for revenue)

status (e.g., booked, cancelled, checked-in)

5. Orchestration

Tool: Apache Airflow (running on Docker)

Purpose: Automate Talend job execution and dependencies

Action:

DAG created to run Talend jobs in sequence

Handles dimension table loads before fact table

Example order:

Load dim_passengers

Load dim_flights

Load dim_tickets

Load dim_date

Load fact_booking

Configuration:

Docker containers connected to SQL Server

Credentials and paths configured for job execution

🔄 End-to-End Flow

Generate raw CSVs → Load into Bronze layer (SQL Server)

Refine and clean → Silver layer

Transform with Talend → Load dimensions and fact into Gold layer

Orchestrate with Airflow → DAG schedules and executes ETL pipeline

🚀 Tech Stack

Python (data generation using Pandas + Faker)

SQL Server (data warehouse storage)

Talend (ETL jobs for Gold layer)

Apache Airflow (workflow orchestration, Docker-based)
