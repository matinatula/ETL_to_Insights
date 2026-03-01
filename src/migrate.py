import os
import logging
import psycopg2
from dotenv import load_dotenv

# Load .env file
load_dotenv()

DB_HOST = os.environ["POSTGRES_HOST"]
DB_PORT = os.environ["POSTGRES_PORT"]
DB_NAME = os.environ["POSTGRES_DB"]
DB_USER = os.environ["POSTGRES_USER"]
DB_PASSWORD = os.environ["POSTGRES_PASSWORD"]

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)


def run_migration(file_path, conn):
    with open(file_path, "r") as f:
        sql = f.read()

    cur = conn.cursor()
    try:
        cur.execute(sql)
        conn.commit()
        logging.info(f"Successfully ran {file_path}")
    except Exception as e:
        conn.rollback()
        logging.error(f"Failed to run {file_path}: {e}")
    finally:
        cur.close()


def main():
    migrations_folder = os.path.join(
        os.path.dirname(__file__), "../migrations")
    migration_files = sorted([os.path.join(migrations_folder, f)
                             for f in os.listdir(migrations_folder) if f.endswith(".sql")])

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

    for file_path in migration_files:
        run_migration(file_path, conn)

    conn.close()
    logging.info("All migrations completed.")


if __name__ == "__main__":
    main()
