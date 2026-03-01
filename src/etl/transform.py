import pandas as pd
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# Employee Transform Function


def transform_employee(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean employee DataFrame
    """
    if df.empty:
        logging.warning("Empty employee DataFrame received")
        return df

    # Trim all string columns
    for col in df.select_dtypes(include="object"):
        df[col] = df[col].astype(str).str.strip()

    # Drop duplicates
    df.drop_duplicates(subset="client_employee_id", inplace=True)

    # Drop columns with all NULL
    df.dropna(axis=1, how='all', inplace=True)

    # Drop rows with all NULL
    df.dropna(axis=0, how='all', inplace=True)

    # Ensure client_employee_id is not null
    df = df[df['client_employee_id'].notna()]

    # Convert dates
    date_cols = ['dob', 'hire_date', 'recent_hire_date',
                 'anniversary_date', 'term_date', 'job_start_date']
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # Convert numeric columns
    num_cols = ['years_of_experience',
                'scheduled_weekly_hour', 'active_status']
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    logging.info(f"Employee DataFrame transformed: shape={df.shape}")
    return df


# Timesheet Transform Function

def transform_timesheet(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean timesheet DataFrame and add derived columns
    """
    if df.empty:
        logging.warning("Empty timesheet DataFrame received")
        return df

    # Trim string columns
    for col in df.select_dtypes(include="object"):
        df[col] = df[col].astype(str).str.strip()

    # Drop duplicates
    df.drop_duplicates(inplace=True)

    # Drop columns/rows with all NULL
    df.dropna(axis=1, how='all', inplace=True)
    df.dropna(axis=0, how='all', inplace=True)

    # Ensure client_employee_id is not null
    df = df[df['client_employee_id'].notna()]

    # Convert dates/timestamps
    date_cols = ['punch_apply_date', 'punch_in_datetime', 'punch_out_datetime',
                 'scheduled_start_datetime', 'scheduled_end_datetime']
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # Convert numeric columns
    num_cols = ['hours_worked']
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')


    # Derived Columns

    # Late flag: 1 if punch_in after scheduled_start, else 0
    df['late_flag'] = ((df['punch_in_datetime'] > df['scheduled_start_datetime'])
                       & df['scheduled_start_datetime'].notna()).astype(int)

    # Early departure flag: 1 if punch_out before scheduled_end, else 0
    df['early_departure_flag'] = ((df['punch_out_datetime'] < df['scheduled_end_datetime'])
                                  & df['scheduled_end_datetime'].notna()).astype(int)

    # Overtime flag: 1 if hours_worked > scheduled_weekly_hour / 5 (assuming 5 workdays)
    df['overtime_flag'] = ((df['hours_worked'] > df['scheduled_weekly_hour'] / 5)
                           & df['scheduled_weekly_hour'].notna()).astype(int)

    logging.info(
        f"Timesheet DataFrame transformed with derived columns: shape={df.shape}")
    return df
