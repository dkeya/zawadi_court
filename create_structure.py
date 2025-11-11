from pathlib import Path
import textwrap
import json
import os

# ====== CHANGE ONLY IF YOU WANT A DIFFERENT ROOT FOLDER ======
BASE_DIR = Path(r"C:\Users\dkeya\Documents\projects\zawadi_court")
# =============================================================

def write(path: Path, content: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text(content.strip() + "\n", encoding="utf-8")

def main():
    # ---------- 1) Top-level folders ----------
    folders = [
        BASE_DIR / "app",
        BASE_DIR / "app" / "persistence",
        BASE_DIR / "app" / "pages",
        BASE_DIR / "app" / "assets",
        BASE_DIR / "configs",
        BASE_DIR / "db",
        BASE_DIR / "db" / "migrations",
        BASE_DIR / "db" / "seeds",
        BASE_DIR / "scripts",
        BASE_DIR / "tests",
        BASE_DIR / "docs",
        BASE_DIR / "data" / "legacy",      # (optional) where your current CSVs can sit
        BASE_DIR / "backups",              # local backups (dev only; Streamlit Cloud is ephemeral)
        BASE_DIR / "receipts",             # uploaded receipts (dev only)
        BASE_DIR / ".streamlit"
    ]
    for f in folders:
        f.mkdir(parents=True, exist_ok=True)

    # ---------- 2) .gitignore ----------
    gitignore = textwrap.dedent("""
        # OS / editor
        .DS_Store
        Thumbs.db
        .vscode/
        .idea/

        # Python
        __pycache__/
        *.pyc
        .pytest_cache/

        # Env & secrets
        .env
        .env.local
        .streamlit/secrets.toml

        # Local data (dev-only)
        data/
        backups/
        receipts/
        *.xlsx

        # Build artifacts
        dist/
        build/
    """)
    write(BASE_DIR / ".gitignore", gitignore)

    # ---------- 3) README ----------
    readme = textwrap.dedent("""
        # Zawadi Court Welfare System

        This repo contains the Streamlit app and a managed-database (Supabase/Postgres) backend.

        ## What lives where
        - `app/` — Streamlit app code
          - `persistence/` — database adapters (Supabase/Postgres), thin data-access layer
          - `pages/` — optional multipage structure (if you split the UI)
          - `assets/` — images, logos, icons
        - `configs/` — configuration files (rates, constants, etc.); keep non-secrets here
        - `db/` — SQL schema, seeds, and migrations (for Postgres/Supabase)
        - `scripts/` — helper scripts (e.g., import from legacy CSV to Supabase)
        - `tests/` — unit tests (optional)
        - `docs/` — documentation, notes, checklists
        - `data/legacy/` — your existing CSVs (local/dev only; the cloud disk is ephemeral)
        - `.streamlit/` — Streamlit config + *secrets.toml* (for cloud deployment)

        ## Setup (local)
        1. Create and activate a virtual environment (recommended).
        2. `pip install -r requirements.txt`
        3. Copy `.env.example` to `.env` and fill values (local dev).
        4. Copy `.streamlit/secrets.toml.example` to `.streamlit/secrets.toml` and fill values (for Streamlit Cloud).
        5. Run app: `streamlit run app/streamlit_app.py`

        ## Production persistence
        We will use **Supabase (Postgres)** as the system of record. CSVs are for legacy import/export only.
    """)
    write(BASE_DIR / "README.md", readme)

    # ---------- 4) Requirements ----------
    # Notes:
    # - psycopg2-binary: direct Postgres access (robust).
    # - or use "supabase" SDK; we'll start with psycopg2 for stability.
    requirements = textwrap.dedent("""
        streamlit>=1.36
        pandas>=2.2
        numpy>=1.26
        plotly>=5.22
        psycopg2-binary>=2.9
        python-dotenv>=1.0
        gspread>=6.1
        google-auth>=2.34
        google-auth-oauthlib>=1.2
        google-auth-httplib2>=0.2
        filelock>=3.15
        xlsxwriter>=3.2
    """)
    write(BASE_DIR / "requirements.txt", requirements)

    # ---------- 5) Streamlit app entry ----------
    streamlit_app = textwrap.dedent("""
        import streamlit as st
        import pandas as pd
        import numpy as np
        from datetime import datetime
        from app.persistence.data_access import get_db, close_db
        from app.persistence.dao import (
            load_reference_rates,
            get_contributions_df,
            get_expenses_df,
            get_special_df,
        )

        st.set_page_config(
            page_title="Zawadi Court Welfare System",
            page_icon="🏠",
            layout="wide",
            initial_sidebar_state="expanded",
        )

        # Minimal placeholder UI; we will replace this by moving your current 1600+ lines here gradually
        st.title("Zawadi Court Welfare System")
        st.caption("Supabase (Postgres) — Managed Database Version")

        db = get_db()  # opens a connection using secrets/env
        try:
            col1, col2, col3 = st.columns(3)
            with col1:
                st.subheader("Rates")
                st.dataframe(load_reference_rates(db))

            with col2:
                st.subheader("Contributions (sample view)")
                st.dataframe(get_contributions_df(db).head(5))

            with col3:
                st.subheader("Expenses (sample view)")
                st.dataframe(get_expenses_df(db).head(5))

            st.subheader("Special Contributions (sample view)")
            st.dataframe(get_special_df(db).head(5))

            st.info("This is a skeleton page. We'll swap your existing UI in, unchanged, then point it to the database.")
        finally:
            close_db(db)
    """)
    write(BASE_DIR / "app" / "streamlit_app.py", streamlit_app)

    # ---------- 6) Data Access Layer (thin; DB connection mgmt) ----------
    data_access = textwrap.dedent("""
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
    """)
    write(BASE_DIR / "app" / "persistence" / "data_access.py", data_access)

    # ---------- 7) DAO (queries we’ll expand as we migrate your UI) ----------
    dao = textwrap.dedent("""
        import pandas as pd

        # Simple SELECT helpers; we'll add INSERT/UPDATE during migration.

        def load_reference_rates(conn):
            q = "SELECT rate_category, amount FROM rates ORDER BY rate_category"
            return pd.read_sql(q, conn)

        def get_contributions_df(conn, limit=None):
            q = "SELECT * FROM contributions ORDER BY family_name, house_no"
            if limit:
                q += f" LIMIT {int(limit)}"
            return pd.read_sql(q, conn)

        def get_expenses_df(conn, limit=None):
            q = "SELECT * FROM expenses ORDER BY date DESC"
            if limit:
                q += f" LIMIT {int(limit)}"
            return pd.read_sql(q, conn)

        def get_special_df(conn, limit=None):
            q = "SELECT * FROM special_contributions ORDER BY date DESC"
            if limit:
                q += f" LIMIT {int(limit)}"
            return pd.read_sql(q, conn)
    """)
    write(BASE_DIR / "app" / "persistence" / "dao.py", dao)

    # ---------- 8) Example page (optional multi-page layout) ----------
    page_placeholder = textwrap.dedent("""
        import streamlit as st
        st.title("Placeholder Page")
        st.write("Additional pages can go here if you split the UI.")
    """)
    write(BASE_DIR / "app" / "pages" / "00_Placeholder.py", page_placeholder)

    # ---------- 9) Streamlit secrets example ----------
    secrets_example = textwrap.dedent("""
        # Copy this to .streamlit/secrets.toml and fill real values for Streamlit Cloud.
        # Postgres (Supabase managed)
        pg_host = "your-project.supabase.co"
        pg_database = "postgres"
        pg_user = "postgres"
        pg_password = "your-strong-password"
        pg_port = 5432

        # (Optional) Supabase REST if we decide to use it later
        supabase_url = "https://your-project.supabase.co"
        supabase_anon_key = "YOUR_SUPABASE_ANON_KEY"
    """)
    write(BASE_DIR / ".streamlit" / "secrets.toml.example", secrets_example)

    # ---------- 10) .env example (local dev only) ----------
    env_example = textwrap.dedent("""
        # Local development Postgres connection (e.g., if you run a local DB or connect directly to Supabase)
        PGHOST=your-project.supabase.co
        PGDATABASE=postgres
        PGUSER=postgres
        PGPASSWORD=your-strong-password
        PGPORT=5432
        PGSSLMODE=require
    """)
    write(BASE_DIR / ".env.example", env_example)

    # ---------- 11) Database schema (Postgres) ----------
    # Matches your CSV structure; we'll import your legacy CSVs into these tables.
    schema_sql = textwrap.dedent("""
        -- Drop tables (for clean re-runs in dev)
        -- DROP TABLE IF EXISTS contributions, expenses, special_contributions, rates,
        --   expense_requests, contribution_requests, special_requests, cash_management CASCADE;

        CREATE TABLE IF NOT EXISTS rates (
          rate_category TEXT PRIMARY KEY,
          amount NUMERIC(14,2) NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS contributions (
          id BIGSERIAL PRIMARY KEY,
          house_no TEXT NOT NULL,
          family_name TEXT NOT NULL,
          lane TEXT,
          rate_category TEXT REFERENCES rates(rate_category) ON UPDATE CASCADE ON DELETE SET NULL,
          email TEXT,
          cumulative_debt_prior NUMERIC(14,2) DEFAULT 0,

          jan NUMERIC(14,2) DEFAULT 0, feb NUMERIC(14,2) DEFAULT 0, mar NUMERIC(14,2) DEFAULT 0,
          apr NUMERIC(14,2) DEFAULT 0, may NUMERIC(14,2) DEFAULT 0, jun NUMERIC(14,2) DEFAULT 0,
          jul NUMERIC(14,2) DEFAULT 0, aug NUMERIC(14,2) DEFAULT 0, sep NUMERIC(14,2) DEFAULT 0,
          oct NUMERIC(14,2) DEFAULT 0, nov NUMERIC(14,2) DEFAULT 0, dec NUMERIC(14,2) DEFAULT 0,

          ytd NUMERIC(14,2) DEFAULT 0,
          current_debt NUMERIC(14,2) DEFAULT 0,
          remarks TEXT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS expenses (
          id BIGSERIAL PRIMARY KEY,
          date DATE NOT NULL,
          description TEXT NOT NULL,
          category TEXT,
          vendor TEXT,
          phone TEXT,
          amount_kes NUMERIC(14,2) NOT NULL DEFAULT 0,
          mode TEXT,
          remarks TEXT,
          receipt TEXT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS special_contributions (
          id BIGSERIAL PRIMARY KEY,
          event TEXT NOT NULL,
          date DATE NOT NULL,
          type TEXT,
          contributors TEXT,
          amount NUMERIC(14,2) NOT NULL DEFAULT 0,
          remarks TEXT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS expense_requests (
          id BIGSERIAL PRIMARY KEY,
          date DATE NOT NULL,
          description TEXT NOT NULL,
          category TEXT,
          requested_by TEXT,
          amount_kes NUMERIC(14,2) DEFAULT 0,
          status TEXT DEFAULT 'Pending Approval',
          remarks TEXT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS contribution_requests (
          id BIGSERIAL PRIMARY KEY,
          date DATE NOT NULL,
          month TEXT NOT NULL,
          family_name TEXT NOT NULL,
          house_no TEXT,
          lane TEXT,
          rate_category TEXT,
          amount_kes NUMERIC(14,2) DEFAULT 0,
          status TEXT DEFAULT 'Pending Approval',
          remarks TEXT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS special_requests (
          id BIGSERIAL PRIMARY KEY,
          date DATE NOT NULL,
          event TEXT NOT NULL,
          type TEXT,
          requested_by TEXT,
          amount NUMERIC(14,2) DEFAULT 0,
          status TEXT DEFAULT 'Pending Approval',
          remarks TEXT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS cash_management (
          id BIGSERIAL PRIMARY KEY,
          cash_balance_cd NUMERIC(14,2) DEFAULT 0,
          cash_withdrawal NUMERIC(14,2) DEFAULT 0,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        -- Seed default rates if empty
        INSERT INTO rates(rate_category, amount)
        VALUES
          ('Resident', 2000),
          ('Non-Resident', 1000),
          ('Special Rate', 500)
        ON CONFLICT (rate_category) DO NOTHING;

        -- Update triggers (optional for updated_at)
        CREATE OR REPLACE FUNCTION set_updated_at()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = now();
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;

        DO $$
        BEGIN
          IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'contributions_set_updated_at') THEN
            CREATE TRIGGER contributions_set_updated_at
            BEFORE UPDATE ON contributions
            FOR EACH ROW EXECUTE FUNCTION set_updated_at();
          END IF;

          IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'expenses_set_updated_at') THEN
            CREATE TRIGGER expenses_set_updated_at
            BEFORE UPDATE ON expenses
            FOR EACH ROW EXECUTE FUNCTION set_updated_at();
          END IF;

          IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'special_set_updated_at') THEN
            CREATE TRIGGER special_set_updated_at
            BEFORE UPDATE ON special_contributions
            FOR EACH ROW EXECUTE FUNCTION set_updated_at();
          END IF;
        END$$;
    """)
    write(BASE_DIR / "db" / "schema.sql", schema_sql)

    # ---------- 12) Seed example data ----------
    seeds_sql = textwrap.dedent("""
        -- Minimal demo rows (remove in production)
        INSERT INTO contributions (house_no, family_name, lane, rate_category, email)
        VALUES ('A01', 'Family Alpha', 'ROYAL', 'Resident', 'alpha@example.com')
        ON CONFLICT DO NOTHING;

        INSERT INTO expenses (date, description, category, vendor, amount_kes, mode)
        VALUES (CURRENT_DATE, 'Gate Repair', 'Maintenance', 'John Doe', 5000, 'Cash');

        INSERT INTO special_contributions (event, date, type, contributors, amount)
        VALUES ('Baby Shower', CURRENT_DATE, 'Celebration', 'Family Alpha', 3000);
    """)
    write(BASE_DIR / "db" / "seeds" / "example_data.sql", seeds_sql)

    # ---------- 13) Helper scripts: apply schema & seeds ----------
    apply_schema = textwrap.dedent("""
        # Apply schema and seeds to the configured Postgres DB.
        # Usage: python scripts\\apply_schema.py
        import os, psycopg2, sys
        from pathlib import Path
        from psycopg2.extras import RealDictCursor

        ROOT = Path(__file__).resolve().parents[1]
        schema_path = ROOT / "db" / "schema.sql"
        seeds_path  = ROOT / "db" / "seeds" / "example_data.sql"

        def get_conn():
            host = os.getenv("PGHOST")
            dbname = os.getenv("PGDATABASE")
            user = os.getenv("PGUSER")
            password = os.getenv("PGPASSWORD")
            port = int(os.getenv("PGPORT", "5432"))
            sslmode = os.getenv("PGSSLMODE", "require")
            if not all([host, dbname, user, password]):
                print("Missing DB env vars. Fill .env or set Streamlit secrets.", file=sys.stderr)
                sys.exit(1)
            return psycopg2.connect(host=host, dbname=dbname, user=user, password=password, port=port, sslmode=sslmode, cursor_factory=RealDictCursor)

        def run_sql(conn, path: Path):
            with conn.cursor() as cur:
                cur.execute(path.read_text(encoding="utf-8"))
            conn.commit()

        if __name__ == "__main__":
            conn = get_conn()
            try:
                run_sql(conn, schema_path)
                run_sql(conn, seeds_path)
                print("Schema and seeds applied successfully.")
            finally:
                conn.close()
    """)
    write(BASE_DIR / "scripts" / "apply_schema.py", apply_schema)

    # ---------- 14) Import legacy CSV -> Postgres (skeleton; we’ll fill later) ----------
    import_legacy = textwrap.dedent("""
        # Import from data/legacy/*.csv into Postgres tables.
        # We'll fill the column mappings together once your CSVs are in data/legacy.
        import os, sys
        from pathlib import Path
        import pandas as pd
        import psycopg2

        ROOT = Path(__file__).resolve().parents[1]
        LEGACY = ROOT / "data" / "legacy"

        def main():
            print("Place your legacy CSVs in:", LEGACY)
            print("We will map and import them in the next step.")
            # Example:
            # df = pd.read_csv(LEGACY / "contributions.csv")
            # ... transform columns -> match db schema ...
            # use psycopg2.extras.execute_values for fast inserts

        if __name__ == "__main__":
            main()
    """)
    write(BASE_DIR / "scripts" / "import_legacy_csvs.py", import_legacy)

    # ---------- 15) Docs starter ----------
    getting_started = textwrap.dedent("""
        # Getting Started (Local)

        1) Install Python 3.10+ and ensure 'python' works in Command Prompt.
        2) Create a virtual environment (recommended).
        3) `pip install -r requirements.txt`
        4) Copy `.env.example` to `.env` and fill PGHOST, PGUSER, etc.
        5) Run: `python scripts\\apply_schema.py` to create tables in your DB.
        6) Run the app: `streamlit run app/streamlit_app.py`
    """)
    write(BASE_DIR / "docs" / "GETTING_STARTED.md", getting_started)

    # ---------- 16) Placeholder test ----------
    test_placeholder = textwrap.dedent("""
        def test_placeholder():
            assert 1 + 1 == 2
    """)
    write(BASE_DIR / "tests" / "test_placeholder.py", test_placeholder)

    print(f"Project scaffold created at: {BASE_DIR}")

if __name__ == "__main__":
    main()
