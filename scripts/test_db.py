import os
import psycopg2
from dotenv import load_dotenv, find_dotenv

# Load .env and OVERRIDE any existing env vars
env_path = find_dotenv(filename=".env", usecwd=True)
if not env_path:
    raise SystemExit("Could not find .env in current directory")

load_dotenv(env_path, override=True)

# Debug: show what we're actually using (mask password)
print("PGHOST =", os.getenv("PGHOST"))
print("PGUSER =", os.getenv("PGUSER"))
print("PGDATABASE =", os.getenv("PGDATABASE"))
print("PGPORT =", os.getenv("PGPORT"))
print("PGSSLMODE =", os.getenv("PGSSLMODE"))
print("PGPASSWORD =", "*" * len(os.getenv("PGPASSWORD") or ""))

try:
    conn = psycopg2.connect(
        host=os.getenv("PGHOST"),
        dbname=os.getenv("PGDATABASE"),
        user=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD"),
        port=os.getenv("PGPORT"),
        sslmode=os.getenv("PGSSLMODE", "require"),
    )
    with conn.cursor() as cur:
        cur.execute("select now(), version();")
        print(cur.fetchone())
    conn.close()
    print("OK")
except Exception as e:
    raise SystemExit(f"Failed to connect: {e}")
