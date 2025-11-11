import os
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
import streamlit as st

# Connection config priority:
# 1) Streamlit Cloud secrets
# 2) .env environment variables (local)
def get_conn_params():
    # Streamlit secrets (production)
    s = getattr(st, "secrets", {})
    if s:
        host = s.get("pg_host")
        dbname = s.get("pg_database")
        user = s.get("pg_user")
        password = s.get("pg_password")
        port = s.get("pg_port", 5432)
        if host and dbname and user and password:
            return dict(host=host, dbname=dbname, user=user, password=password, port=port, sslmode="require")

    # Environment variables (local dev)
    host = os.getenv("PGHOST")
    dbname = os.getenv("PGDATABASE")
    user = os.getenv("PGUSER")
    password = os.getenv("PGPASSWORD")
    port = os.getenv("PGPORT", "5432")
    sslmode = os.getenv("PGSSLMODE", "prefer")
    if host and dbname and user and password:
        return dict(host=host, dbname=dbname, user=user, password=password, port=int(port), sslmode=sslmode)

    raise RuntimeError("Database connection settings not found. Check .streamlit/secrets.toml or .env variables.")

def get_db():
    params = get_conn_params()
    conn = psycopg2.connect(**params, cursor_factory=RealDictCursor)
    conn.autocommit = True
    return conn

def close_db(conn):
    try:
        conn.close()
    except Exception:
        pass
