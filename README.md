# Customer Analytics Pipeline

ETL pipeline extracting data from Snowflake, cleaning with Pandas, loading to PostgreSQL, exposed via FastAPI REST API with GitHub Actions CI/CD.

## Tech Stack
- Snowflake (data warehouse)
- Python + Pandas (ETL)
- PostgreSQL (cleaned data storage)
- FastAPI (REST API)
- pytest (automated tests)
- GitHub Actions (CI/CD)

## Pipeline Flow
Snowflake → Extract → Clean → PostgreSQL → FastAPI

## API Endpoints
- GET /health
- POST /run-pipeline
- GET /customers
- GET /customers/churned
- GET /customers/summary