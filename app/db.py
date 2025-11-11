# app/db.py
import os
import psycopg2
from dotenv import load_dotenv, find_dotenv

# Load .env once (works for Streamlit or scripts)
load_dotenv(find_dotenv(filename=".env", usecwd=True), override=True)

def get_conn():
    """
    Open a new psycopg2 connection using environment variables.
    Close it after use (with context manager) to avoid leaks.
    """
    return psycopg2.connect(
        host=os.getenv("PGHOST"),
        dbname=os.getenv("PGDATABASE"),
        user=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD"),
        port=os.getenv("PGPORT", "5432"),
        sslmode=os.getenv("PGSSLMODE", "require"),
    )
