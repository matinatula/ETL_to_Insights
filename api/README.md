# API Setup

## 1) Install dependencies

```bash
pip install -r requirements.txt
```

## 2) Run migrations

```bash
python src/migrate.py
```

This will create `employee`, `timesheet`, and `users` tables.

## 3) Start API server

```bash
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
```

## Authentication flow

1. Call `POST /auth/bootstrap-admin` once to create the first admin user.
2. Call `POST /auth/login` to get JWT token.
3. Use `Authorization: Bearer <token>` for protected endpoints.

## Endpoints

- `POST /auth/bootstrap-admin` - create first admin (one-time)
- `POST /auth/register` - admin creates users (admin/viewer)
- `POST /auth/login` - returns JWT
- `GET /health` - health check

### Employee API (CRUD)

- `POST /employees` (admin)
- `GET /employees` (admin/viewer)
- `GET /employees/{employee_id}` (admin/viewer)
- `PUT /employees/{employee_id}` (admin)
- `DELETE /employees/{employee_id}` (admin)

### Timesheet API (read-only)

- `GET /timesheets?employee_id=<id>&start_date=YYYY-MM-DD&end_date=YYYY-MM-DD`
- `GET /timesheets/employee/{employee_id}?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD`

Both endpoints are read-only and available to authenticated users.
