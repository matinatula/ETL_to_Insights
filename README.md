# ETL to Insights - Project Documentation

## Overview

This project implements an HR analytics data platform using a medallion-style ETL pipeline and a secure REST API.

- **Data pipeline goal:** The data pipeline is designed to ingest employee and timesheet data, standardize quality, and publish analytics-ready datasets.
- **Serving goal:** The operational data is exposed through authenticated APIs with role-based access control.
- **Output goal:** KPI CSV outputs and interactive HTML reports for stakeholders are to be generated.

---

## 1) Engineering Decisions

### 1.1 Technology Choices

1. I used **Python** as the primary language for ETL and API development because I wanted the entire stack consisting of ingestion, transformation, API, and orchestration to live in one language without context-switching. I considered Java and Scala since they're common in enterprise data engineering, but the boilerplate overhead and slower iteration cycle weren't worth it for a project at this scale. Python's ecosystem (pandas, SQLAlchemy, FastAPI, Airflow) already covered everything I needed, and keeping it consistent made debugging and dependency management significantly simpler.

2. I chose **PostgreSQL** as the analytical and operational data store because I needed a database that could handle relational integrity, complex joins, and window functions across the Silver and Gold layers, not just simple reads and writes. I ruled out SQLite early since it lacks concurrent write support and struggles with analytical queries at scale. Cloud warehouses like BigQuery or Snowflake were overkill for a local-first batch pipeline and would have added unnecessary infrastructure cost. Between MySQL and PostgreSQL, I chose PostgreSQL because of its stricter type enforcement, better constraint handling, and stronger support for the kind of data quality guarantees that matter when cleaning messy HR source data.

3. I went with **Apache Airflow** for orchestration of medallion ETL stages because I needed proper DAG-level dependency management between Bronze, Silver, Gold, not just a scheduled script. I initially considered using a cron job since the pipeline is batch-based, but cron has no concept of task dependencies, no per-task retry logic, and no visibility into which stage failed without digging through logs manually. I also looked at Prefect and Luigi. Prefect has a cleaner API but requires a cloud backend for full observability, and Luigi is simpler but lacks a built-in UI. Airflow gave me everything I needed (dependency management, retries, a web UI) without requiring anything beyond Docker.

4. I settled on **Pandas + SQLAlchemy** for transformation logic and table loading because the source CSV data had enough quality issues like inconsistent date formats, mixed-type columns, encoding inconsistencies, and handling them in pure SQL stored procedures would have been painful. Pandas made the type coercion, null handling, and conditional cleaning logic much easier to read and debug. SQLAlchemy was paired with it specifically to handle connection pooling and chunked table writes, so I wasn't loading entire result sets into memory at once. The trade-off is higher memory overhead compared to a pure SQL pipeline, but the readability and debuggability were worth it at this scale.

5. I decided on **FastAPI** for REST API implementation because I wanted input validation, serialization, and auto-generated API documentation without having to wire them up manually. I considered Flask first since it's familiar and lightweight, but it would have required additional libraries (Marshmallow or Pydantic, Flask-RESTX) to get the same features FastAPI provides out of the box. Django REST Framework was ruled out as too opinionated and heavyweight for an API with a small surface area. FastAPI's async support also means the API handles concurrent requests more efficiently, which matters if the timesheet endpoints end up being hit heavily by a reporting layer.

6. I sided with **JWT (PyJWT)** for stateless authentication and role-aware authorization because I didn't want to manage server-side session state. Session-based auth would have required either in-memory storage or a session store like Redis, which adds infrastructure and complicates scaling. I considered OAuth2 for the role-based access requirements, but the implementation overhead: authorization server, token exchange flows is disproportionate for an internal API with only two roles (`admin` and `viewer`). JWT lets me carry role claims directly in the token payload and validate them on every request without a database lookup. The main trade-off is that token revocation is indirect, but with a short expiry window configured in the environment, this was an acceptable compromise.

7. I opted for **Docker Compose** for consistent local deployment of Airflow and PostgreSQL because I wanted the entire infrastructure to be reproducible across machines with a single command. Running everything bare-metal would have meant manual version pinning, OS-specific configuration, and the inevitable "works on my machine" problems. I considered Kubernetes briefly but it's far too complex for local development orchestration. Docker Compose let me declare the full stack: PostgreSQL, Airflow webserver, Airflow scheduler, all in one file and spin it up reliably regardless of the host environment.

### 1.2 Architecture Decisions

- **Medallion pattern (Bronze/Silver/Gold):**
  - **Bronze:** The bronze layer involves raw ingestion into `staging_employee`, `staging_timesheet`.
  - **Silver:** The silver layer cleans relational tables `employee`, `timesheet`.
  - **Gold:** The gold layer derives analytics table `timesheet_derived` with attendance behavior flags.
- **Separation of concerns:**
  - `src/etl/*` is used for ingestion/transformation/derivation.
  - `src/analytics/visualizations.py` handles KPI extraction and report generation.
  - `api/*` works with auth, authorization, and data-serving endpoints.
- **Security model:**
  - `users` table stores credentials as salted PBKDF2 password hashes.
  - JWT carries `sub` (username) and `role` claims.
  - Role policy: `admin` can mutate employee/user data, `viewer` has read access.

---

## 2) Setup Instructions (Step-by-Step)

### 2.1 Prerequisites

- Python 3.10+
- Docker + Docker Compose
- Git

### 2.2 Clone and Prepare

```bash
git clone https://github.com/matinatula/ETL_to_Insights
cd ETL_to_Insights
```

### 2.3 Configure Environment

Create or update `.env` with these keys (values should match your environment):

```env
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
ETL_POSTGRES_DB=etl_db
POSTGRES_DB=airflow_metadata

AIRFLOW_FERNET_KEY=<fernet_key>
AIRFLOW_ADMIN_PASSWORD=<airflow_admin_password>

SOURCE_TYPE=local
EMPLOYEE_CSV=data/employee_202510161125.csv
TIMESHEETS_FOLDER=data/timesheets

# Optional for MinIO source mode
MINIO_ENDPOINT=
MINIO_ACCESS_KEY=
MINIO_SECRET_KEY=
MINIO_BUCKET=
MINIO_EMPLOYEE_OBJECT=
MINIO_TIMESHEETS_PREFIX=

# API auth settings
JWT_SECRET_KEY=<strong_secret>
JWT_EXPIRES_MINUTES=60
```

Generate an `AIRFLOW_FERNET_KEY` (if you do not already have one):

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Notes:
- `POSTGRES_DB` is for the Airflow metadata DB.
- `ETL_POSTGRES_DB` is for ETL domain data (`employee`, `timesheet`, etc.).
- If they use different names, create `ETL_POSTGRES_DB` before running migrations.
- Use a long random value for `JWT_SECRET_KEY` in non-local environments.

Create ETL DB (only required when it does not already exist):

```bash
docker exec -it etl-postgres psql -U "$POSTGRES_USER" -d postgres -c "CREATE DATABASE etl_db;"
```

### 2.4 Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 2.5 Start Infrastructure

```bash
docker compose up -d db airflow-init airflow-webserver airflow-scheduler
```

Validate services:

```bash
docker compose ps
```

### 2.6 Run Database Migrations

```bash
python src/migrate.py
```

This applies all files in `migrations/` including:
- core entities: `employee`, `timesheet`
- derived layer: `timesheet_derived`
- staging layer tables
- indexes
- API auth table: `users`

---

## 3) Usage Guide

### 3.1 Run the ETL Pipeline (Airflow)

1. Open Airflow UI at `http://localhost:8080`.
2. Log in with the credentials configured for Airflow init.
3. Locate DAG: `etl_pipeline`.
4. Trigger a manual run.

Pipeline stages:
- `extract_employee` and `extract_timesheets` load Bronze data.
- `transform_employee` and `transform_timesheet` clean/load Silver data.
- `derive_gold` computes attendance behavior flags into Gold.

### 3.2 Generate KPI Reports

Run:

```bash
python -m src.analytics.visualizations
```

Outputs:
- In order to see the individual csv files for the KPIs: `reports/csv/`
- Interactive chart HTML files: `reports/interactive/`
- Consolidated interactive dashboard page: `reports/index.html`

### 3.3 Run the API

```bash
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
```

Open API docs:
- Swagger UI: `http://localhost:8000/docs`
- OpenAPI spec: `http://localhost:8000/openapi.json`

Authentication flow:
1. `POST /auth/bootstrap-admin` (one-time initial admin creation)
2. `POST /auth/login` to receive JWT token
3. Send `Authorization: Bearer <token>` to protected endpoints

---

## 4) Schema Documentation and Entity Relationships

### 4.1 Core Entities

- **`employee`** (dimension)
  - PK: `client_employee_id`

- **`timesheet`** (fact)
  - PK: `timesheet_id`
  - FK: `client_employee_id -> employee.client_employee_id`

- **`timesheet_derived`** (gold analytics)
  - Derived from `timesheet` (+ employee schedule context)
  - Adds `late_flag`, `early_departure_flag`, `overtime_flag`.

- **`users`** (API security)
  - PK: `user_id`
  - Unique: `username`
  - Contains password hash, role, active status.

### 4.2 Staging Entities

- `staging_employee`
- `staging_timesheet`

We need to create ingestion landing tables to be used before Silver-level cleaning and validation.

### 4.3 Relationship Model

![ER Diagram](ER_Diagram.png)


### 4.4 Integrity and Performance Constraints

- FK on `timesheet.client_employee_id` enforces valid employee ownership.
- Indexes:
  - `idx_timesheet_employee` on `timesheet(client_employee_id)`
  - `idx_timesheet_date` on `timesheet(punch_apply_date)`
  - `idx_employee_department` on `employee(department_id)`
  - `idx_users_username` on `users(username)`

---

## 5) API 

The API is designed to satisfy two core requirements. The first one is full CRUD operations for `employee` whereas the second one is read-only access for `timesheet` with employee/date filters.

Authentication and authorization are enforced with JWT and role-based access:
- `admin`: can create/update/delete employee records and register users
- `viewer`: read-only access to employee and timesheet data

All protected routes require `Authorization: Bearer <token>`.

### Employee 

- `POST /employees` (admin)
- `GET /employees` (authenticated)
- `GET /employees/{employee_id}` (authenticated)
- `PUT /employees/{employee_id}` (admin)
- `DELETE /employees/{employee_id}` (admin)

### Timesheet 

- `GET /timesheets`
  - Optional filters: `employee_id`, `start_date`, `end_date`
- `GET /timesheets/employee/{employee_id}`
  - Optional filters: `start_date`, `end_date`

### Auth

- `POST /auth/bootstrap-admin`
- `POST /auth/register` (admin)
- `POST /auth/login`

---


## 6) Operational Notes

- Logs are available in `logs/` for Airflow tasks and scheduler behavior.
- Migrations run in lexical order from `migrations/`.
- Current pipeline is optimized for deterministic batch runs, not streaming ingestion.

---
