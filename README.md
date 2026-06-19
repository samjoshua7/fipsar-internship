# FIPSAR Internship Projects

This repository contains projects, assignments, and learning materials completed during my internship focused on data engineering, workflow orchestration, automation, and backend systems.

The goal of this internship is to gain hands-on experience by building real-world data pipelines and automation workflows using industry-standard tools.

## Repository Structure

```text
fipsar-internship/
├── api-data-ingestion/
│   ├── airflow/
│   ├── dags/
│   ├── docs/
│   ├── logs/
│   ├── scripts/
│   ├── sql/
│   ├── .env.example
│   ├── .gitignore
│   ├── requirements.txt
│   └── README.md
├── sql-data-analysis/
│   ├── SQLQuery1--FIPSAR_Assignment.sql
│   └── README.md
├── LICENSE
├── .gitignore
└── README.md
```

## Projects

### 1. API Data Ingestion Pipeline

Build a real-time data ingestion pipeline that:

* Consumes data from a public REST API
* Processes JSON responses using Python
* Stores data in PostgreSQL
* Automates execution using Apache Airflow
* Runs Airflow in Docker containers using WSL2

### 2. SQL Data Analysis Assignment

Analyze the `AdventureWorksDW2025` data warehouse with T-SQL queries for finance, call center, and customer survey metrics.

* Aggregates financial results by organization, currency, department, scenario, and account
* Reports call center performance by shift, date, and wage type
* Summarizes customer purchase behavior by gender, first purchase date, and age range

## Technology Stack

* Python
* PostgreSQL
* Apache Airflow
* Docker
* WSL2 (Ubuntu)
* Microsoft SQL Server
* SQL Server Management Studio (SSMS) 22
* AdventureWorksDW2025 sample database
* Git & GitHub
* VS Code

## SQL Data Analysis Setup

The SQL assignment uses Microsoft SQL Server and SSMS 22 with the `AdventureWorksDW2025` sample database.

1. Install Microsoft SQL Server Developer Edition or SQL Server Express.
2. Install SQL Server Management Studio 22.
3. Download and restore the `AdventureWorksDW2025` sample database into your local SQL Server instance.
4. Open `sql-data-analysis/SQLQuery1--FIPSAR_Assignment.sql` in SSMS.
5. Connect to `localhost` or your named instance, then execute the script using `AdventureWorksDW2025`.

## Learning Outcomes

Through these projects, I gained practical experience with:

* API integration
* JSON parsing and transformation
* Database schema design
* SQL operations
* Workflow orchestration
* Docker containerization
* Linux environments using WSL2
* Environment variable management
* Git version control
* Debugging distributed systems

## Notes

* Runtime-generated files such as logs, virtual environments, and configuration files are excluded using `.gitignore`.
* Sensitive information such as passwords and connection strings are stored in `.env` files and are not committed to version control.

## License

This repository is licensed under the MIT License.