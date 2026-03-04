from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel, Field


class RegisterRequest(BaseModel):
    username: str = Field(min_length=3, max_length=100)
    password: str = Field(min_length=8, max_length=256)
    role: str = Field(default="viewer")


class LoginRequest(BaseModel):
    username: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


class EmployeeBase(BaseModel):
    first_name: Optional[str] = None
    middle_name: Optional[str] = None
    last_name: Optional[str] = None
    preferred_name: Optional[str] = None
    job_code: Optional[int] = None
    job_title: Optional[str] = None
    job_start_date: Optional[date] = None
    organization_id: Optional[str] = None
    organization_name: Optional[str] = None
    department_id: Optional[str] = None
    department_name: Optional[str] = None
    dob: Optional[date] = None
    hire_date: Optional[date] = None
    recent_hire_date: Optional[date] = None
    anniversary_date: Optional[date] = None
    term_date: Optional[date] = None
    years_of_experience: Optional[int] = None
    work_email: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip: Optional[str] = None
    country: Optional[str] = None
    manager_employee_id: Optional[str] = None
    manager_employee_name: Optional[str] = None
    fte_status: Optional[str] = None
    is_per_deim: Optional[str] = None
    cell_phone: Optional[str] = None
    work_phone: Optional[str] = None
    scheduled_weekly_hour: Optional[int] = None
    active_status: Optional[int] = None
    termination_reason: Optional[str] = None
    clinical_level: Optional[str] = None


class EmployeeCreate(EmployeeBase):
    client_employee_id: str
    first_name: str
    last_name: str
    hire_date: date


class EmployeeUpdate(EmployeeBase):
    pass


class EmployeeResponse(EmployeeBase):
    client_employee_id: str


class TimesheetResponse(BaseModel):
    timesheet_id: int
    client_employee_id: str
    department_id: Optional[str] = None
    department_name: Optional[str] = None
    home_department_id: Optional[str] = None
    home_department_name: Optional[str] = None
    pay_code: Optional[str] = None
    punch_in_comment: Optional[str] = None
    punch_out_comment: Optional[str] = None
    hours_worked: Optional[float] = None
    punch_apply_date: date
    punch_in_datetime: datetime
    punch_out_datetime: datetime
    scheduled_start_datetime: Optional[datetime] = None
    scheduled_end_datetime: Optional[datetime] = None
