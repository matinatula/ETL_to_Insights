from datetime import date
from typing import Any, Dict, List

import jwt
from fastapi import Depends, FastAPI, HTTPException, Query, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from psycopg2 import sql

from api.db import get_connection
from api.schemas import (
    EmployeeCreate,
    EmployeeResponse,
    EmployeeUpdate,
    LoginRequest,
    RegisterRequest,
    TimesheetResponse,
    TokenResponse,
)
from api.security import create_access_token, decode_access_token, hash_password, verify_password


app = FastAPI(title="ETL to Insights API", version="1.0.0")
bearer_scheme = HTTPBearer()


def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
) -> Dict[str, Any]:
    token = credentials.credentials

    try:
        payload = decode_access_token(token)
    except jwt.InvalidTokenError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
        ) from exc

    username = payload.get("sub")
    role = payload.get("role")

    if not username or not role:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token payload is invalid",
        )

    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT username, role, is_active
                FROM users
                WHERE username = %s
                """,
                (username,),
            )
            db_user = cursor.fetchone()

    if not db_user or db_user["is_active"] != 1:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User is invalid or inactive",
        )

    if db_user["role"] != role:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token role no longer matches user role",
        )

    return {"username": db_user["username"], "role": db_user["role"]}


def require_admin(current_user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
    if current_user["role"] != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin role is required",
        )
    return current_user


@app.get("/health")
def health_check() -> Dict[str, str]:
    return {"status": "ok"}


@app.post("/auth/bootstrap-admin", status_code=status.HTTP_201_CREATED)
def bootstrap_admin(payload: LoginRequest) -> Dict[str, str]:
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) AS total FROM users")
            total = cursor.fetchone()["total"]
            if total > 0:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Bootstrap is disabled after first user is created",
                )

            cursor.execute(
                """
                INSERT INTO users (username, password_hash, role)
                VALUES (%s, %s, 'admin')
                """,
                (payload.username, hash_password(payload.password)),
            )

    return {"message": "Bootstrap admin created successfully"}


@app.post("/auth/register", status_code=status.HTTP_201_CREATED)
def register_user(
    payload: RegisterRequest,
    _: Dict[str, Any] = Depends(require_admin),
) -> Dict[str, str]:
    role = payload.role.lower().strip()
    if role not in {"admin", "viewer"}:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Role must be either 'admin' or 'viewer'",
        )

    password_hash = hash_password(payload.password)

    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT username FROM users WHERE username = %s", (payload.username,))
            existing = cursor.fetchone()
            if existing:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Username already exists",
                )

            cursor.execute(
                """
                INSERT INTO users (username, password_hash, role)
                VALUES (%s, %s, %s)
                """,
                (payload.username, password_hash, role),
            )

    return {"message": "User created successfully"}


@app.post("/auth/login", response_model=TokenResponse)
def login(payload: LoginRequest) -> TokenResponse:
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT username, password_hash, role, is_active
                FROM users
                WHERE username = %s
                """,
                (payload.username,),
            )
            user = cursor.fetchone()

    if not user or not verify_password(payload.password, user["password_hash"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
        )

    if user["is_active"] != 1:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User is inactive",
        )

    token = create_access_token(user["username"], user["role"])
    return TokenResponse(access_token=token)


@app.post("/employees", response_model=EmployeeResponse, status_code=status.HTTP_201_CREATED)
def create_employee(payload: EmployeeCreate, _: Dict[str, Any] = Depends(require_admin)) -> EmployeeResponse:
    employee_data = payload.model_dump()
    columns = list(employee_data.keys())
    values = [employee_data[column] for column in columns]

    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT client_employee_id FROM employee WHERE client_employee_id = %s",
                (payload.client_employee_id,),
            )
            if cursor.fetchone():
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Employee already exists",
                )

            insert_sql = sql.SQL(
                "INSERT INTO employee ({fields}) VALUES ({values}) RETURNING *"
            ).format(
                fields=sql.SQL(", ").join(map(sql.Identifier, columns)),
                values=sql.SQL(", ").join(sql.Placeholder() * len(columns)),
            )

            cursor.execute(insert_sql, values)
            row = cursor.fetchone()

    return EmployeeResponse(**row)


@app.get("/employees", response_model=List[EmployeeResponse])
def list_employees(_: Dict[str, Any] = Depends(get_current_user)) -> List[EmployeeResponse]:
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM employee ORDER BY client_employee_id")
            rows = cursor.fetchall()
    return [EmployeeResponse(**row) for row in rows]


@app.get("/employees/{employee_id}", response_model=EmployeeResponse)
def get_employee(employee_id: str, _: Dict[str, Any] = Depends(get_current_user)) -> EmployeeResponse:
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT * FROM employee WHERE client_employee_id = %s",
                (employee_id,),
            )
            row = cursor.fetchone()

    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Employee not found")

    return EmployeeResponse(**row)


@app.put("/employees/{employee_id}", response_model=EmployeeResponse)
def update_employee(
    employee_id: str,
    payload: EmployeeUpdate,
    _: Dict[str, Any] = Depends(require_admin),
) -> EmployeeResponse:
    updates = payload.model_dump(exclude_none=True)
    if not updates:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No fields were provided for update",
        )

    assignments = [sql.SQL("{} = %s").format(sql.Identifier(k)) for k in updates.keys()]
    values = list(updates.values()) + [employee_id]

    query = sql.SQL(
        "UPDATE employee SET {assignments} WHERE client_employee_id = %s RETURNING *"
    ).format(assignments=sql.SQL(", ").join(assignments))

    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, values)
            row = cursor.fetchone()

    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Employee not found")

    return EmployeeResponse(**row)


@app.delete("/employees/{employee_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_employee(employee_id: str, _: Dict[str, Any] = Depends(require_admin)) -> None:
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "DELETE FROM employee WHERE client_employee_id = %s RETURNING client_employee_id",
                (employee_id,),
            )
            deleted = cursor.fetchone()

    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Employee not found")


@app.get("/timesheets", response_model=List[TimesheetResponse])
def list_timesheets(
    employee_id: str | None = Query(default=None),
    start_date: date | None = Query(default=None),
    end_date: date | None = Query(default=None),
    _: Dict[str, Any] = Depends(get_current_user),
) -> List[TimesheetResponse]:
    conditions = []
    params: List[Any] = []

    if employee_id:
        conditions.append("client_employee_id = %s")
        params.append(employee_id)

    if start_date:
        conditions.append("punch_apply_date >= %s")
        params.append(start_date)

    if end_date:
        conditions.append("punch_apply_date <= %s")
        params.append(end_date)

    where_clause = ""
    if conditions:
        where_clause = " WHERE " + " AND ".join(conditions)

    query = (
        "SELECT * FROM timesheet"
        + where_clause
        + " ORDER BY punch_apply_date DESC, timesheet_id DESC"
    )

    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, tuple(params))
            rows = cursor.fetchall()

    return [TimesheetResponse(**row) for row in rows]


@app.get("/timesheets/employee/{employee_id}", response_model=List[TimesheetResponse])
def get_timesheets_by_employee(
    employee_id: str,
    start_date: date | None = Query(default=None),
    end_date: date | None = Query(default=None),
    current_user: Dict[str, Any] = Depends(get_current_user),
) -> List[TimesheetResponse]:
    return list_timesheets(
        employee_id=employee_id,
        start_date=start_date,
        end_date=end_date,
        _=current_user,
    )
