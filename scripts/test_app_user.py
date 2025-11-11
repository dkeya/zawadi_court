# scripts/test_app_user.py
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv, find_dotenv

# Load .env
load_dotenv(find_dotenv(filename=".env", usecwd=True), override=True)

dsn = dict(
    host=os.getenv("PGHOST"),
    dbname=os.getenv("PGDATABASE"),
    user=os.getenv("PGUSER"),
    password=os.getenv("PGPASSWORD"),
    port=os.getenv("PGPORT", "5432"),
    sslmode=os.getenv("PGSSLMODE", "require"),
)

def main():
    print("Connecting as:", dsn["user"])
    with psycopg2.connect(**dsn) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Who am I?
            cur.execute("select current_user, session_user;")
            who = cur.fetchone()
            print("User check:", who)

            # Read checks (should work)
            for tbl in [
                "rates",
                "contributions",
                "expenses",
                "expense_requests",
                "contribution_requests",
                "special",
                "special_requests",
                "cash_management",
            ]:
                try:
                    cur.execute(f"select count(*) as n from {tbl};")
                    n = cur.fetchone()["n"]
                    print(f"✓ SELECT {tbl}: {n} rows")
                except Exception as e:
                    print(f"✗ SELECT {tbl} failed:", e)

        # Write probe in a fresh connection using a TEMP table (auto-dropped)
        try:
            probe_dsn = dsn.copy()
            # use a clean connection for the probe
            with psycopg2.connect(**probe_dsn) as wconn:
                wconn.autocommit = False  # explicit txn we can roll back
                with wconn.cursor() as wcur:
                    wcur.execute("CREATE TEMP TABLE tmp_probe(note text);")
                    wcur.execute("INSERT INTO tmp_probe(note) VALUES (%s);", ("probe",))
                    wconn.rollback()  # discard the temp write
            print("✓ Write test ok (temp table, rolled back).")
        except Exception as e:
            print("✗ Write test failed:", e)

    print("Done.")

if __name__ == "__main__":
    main()
