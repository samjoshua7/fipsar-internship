# SQL Data Analysis Assignment

## Overview

This folder contains a SQL assignment for the FIPSAR internship. It uses the `AdventureWorksDW2025` data warehouse to analyze business metrics across finance, call center, and customer survey data.

The SQL script demonstrates:

* Aggregation and grouping
* Date and time analysis
* Join operations across fact and dimension tables
* Trend analysis by year, quarter, month, and week
* Product, customer, and organizational analysis

## Assignment Scope

The assignment is implemented in `SQLQuery1--FIPSAR_Assignment.sql` and includes queries that answer questions such as:

* How finance totals vary by ownership percentage and organizational structure
* Revenue trends by year, quarter, month, and week
* Top currencies, organizations, departments, scenarios, and accounts by total financial amount
* Call center performance by shift, date, and wage type
* Customer behavior by gender, first purchase date, and product categories
* Age-range analysis for product engagement

## File Contents

* `SQLQuery1--FIPSAR_Assignment.sql` - Main SQL script containing the completed assignment queries.

## Key Tables Used

* `FactFinance`
* `FactCallCenter`
* `FactSurveyResponse`
* `DimOrganization`
* `DimDate`
* `DimCurrency`
* `DimDepartmentGroup`
* `DimScenario`
* `DimAccount`
* `DimCustomer`

## Example Query Topics

* Financial totals by ownership percentage
* Sales performance by time period
* Top financial contributors by organization and currency
* Call volume and average issue resolution times
* Customer response segmentation by gender and marital status
* Product adoption across customer age ranges

## Prerequisites

* Microsoft SQL Server 2022 Developer Edition or SQL Server Express
* SQL Server Management Studio (SSMS) 22
* AdventureWorksDW2025 sample database restored locally
* Access to a Windows environment with SSMS installed

## Setup Instructions

1. Install Microsoft SQL Server 2022 Developer Edition or SQL Server Express.
2. Install SQL Server Management Studio 22.
3. Download the `AdventureWorksDW2025` sample database backup from the Microsoft SQL Server Samples GitHub repository.
4. In SSMS, connect to your local server instance (`localhost`, `localhost\SQLEXPRESS`, or your named instance).
5. Right-click `Databases` and select `Restore Database...`.
6. Choose the `AdventureWorksDW2025` backup file and restore it to your local server.
7. Confirm that the `AdventureWorksDW2025` database appears under `Databases` in SSMS.
8. Open `sql-data-analysis/SQLQuery1--FIPSAR_Assignment.sql` in SSMS.
9. Ensure the script begins with `USE AdventureWorksDW2025;` and execute each query section.

## How to Use

1. Open the SQL file in SQL Server Management Studio 22.
2. Connect to your local SQL Server instance.
3. Verify the `AdventureWorksDW2025` database exists.
4. Execute the queries from top to bottom and review the result sets.

## Notes

* This file is designed for Microsoft SQL Server / T-SQL environments.
* The dataset used is the AdventureWorks data warehouse schema for 2025.
* No external dependencies are required beyond access to the database.
