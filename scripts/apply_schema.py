import os
import psycopg2
from dotenv import load_dotenv, find_dotenv
from pathlib import Path

env_path = find_dotenv(filename=".env", usecwd=True)
load_dotenv(env_path, override=True)

schema_path = Path("db/schema.sql")
if not schema_path.exists():
    raise SystemExit("db/schema.sql not found")

dsn = dict(
    host=os.getenv("PGHOST"),
    dbname=os.getenv("PGDATABASE"),
    user=os.getenv("PGUSER"),
    password=os.getenv("PGPASSWORD"),
    port=os.getenv("PGPORT", "5432"),
    sslmode=os.getenv("PGSSLMODE", "require"),
)

print("Connecting to Supabase…")
with psycopg2.connect(**dsn) as conn:
    with conn.cursor() as cur:
        sql = schema_path.read_text(encoding="utf-8")
        cur.execute(sql)
        conn.commit()
print("✅ Schema applied.")

