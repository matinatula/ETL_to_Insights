import os
import pandas as pd
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


def extract_employee(file_path: str) -> pd.DataFrame:
    """
    Extract employee CSV into a DataFrame
    """
    if not os.path.exists(file_path):
        logging.error(f"Employee file not found: {file_path}")
        return pd.DataFrame()  # return empty df instead of crashing
    try:
        df = pd.read_csv(file_path)
        logging.info(
            f"Employee data extracted from {file_path}, shape={df.shape}")
        return df
    except Exception as e:
        logging.error(f"Error reading employee CSV: {e}")
        return pd.DataFrame()


def extract_timesheets(folder_path: str) -> pd.DataFrame:
    """
    Extract all timesheet CSVs from folder into a single DataFrame
    """
    if not os.path.exists(folder_path):
        logging.error(f"Timesheet folder not found: {folder_path}")
        return pd.DataFrame()

    all_files = [
        os.path.join(folder_path, f)
        for f in os.listdir(folder_path)
        if f.endswith(".csv")
    ]

    if not all_files:
        logging.warning(f"No CSV files found in {folder_path}")
        return pd.DataFrame()

    dfs = []
    for file in all_files:
        try:
            df = pd.read_csv(file)
            logging.info(f"Timesheet extracted from {file}, shape={df.shape}")
            dfs.append(df)
        except Exception as e:
            logging.error(f"Error reading {file}: {e}")

    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
        logging.info(f"Combined timesheets shape={combined_df.shape}")
        return combined_df
    else:
        logging.warning("No timesheets loaded, returning empty DataFrame")
        return pd.DataFrame()
