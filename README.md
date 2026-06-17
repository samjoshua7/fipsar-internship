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

## Technology Stack

* Python
* PostgreSQL
* Apache Airflow
* Docker
* WSL2 (Ubuntu)
* Git & GitHub
* VS Code

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