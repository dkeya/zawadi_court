# C:\Users\dkeya\Documents\projects\zawadi_court\zawadi_db.py

# --- imports (put at very top of zawadi_db.py) ---
import os
import json
import time  # ✅ used in _engine() tiny backoff
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError, ProgrammingError
from contextlib import contextmanager  # ✅ needed for @contextmanager

# ✅ missing previously
import psycopg2
from psycopg2.extras import RealDictCursor

def _build_candidate_urls():
    """
    Try what's in DATABASE_URL first.
    If it looks like a Supabase pooler host, try both 5432 and 6543.
    Otherwise, just return the provided URL.
    """
    raw = (os.getenv("DATABASE_URL") or "").strip()
    if not raw:
        return []

    urls = [raw]

    # If user set 5432 and it times out, try 6543; if set 6543, try 5432 as a backup
    # Only do this for Supabase pooler hosts.
    host_markers = (".pooler.supabase.com",)
    lower = raw.lower()
    if any(m in lower for m in host_markers):
        if ":5432/" in lower:
            urls.append(lower.replace(":5432/", ":6543/"))
        elif ":6543/" in lower:
            urls.append(lower.replace(":6543/", ":5432/"))

    # Ensure common connection parameters are present (short timeouts + keepalives)
    def add_params(u: str) -> str:
        sep = "&" if "?" in u else "?"
        params = (
            "sslmode=require"
            "&connect_timeout=6"
            "&application_name=zawadi_streamlit"
            "&keepalives=1&keepalives_idle=30&keepalives_interval=10&keepalives_count=3"
        )
        # don't duplicate if user already specified sslmode
        if "sslmode=" in u:
            return u
        return f"{u}{sep}{params}"

    return [add_params(u) for u in urls]

def _engine() -> Engine:
    urls = _build_candidate_urls()
    if not urls:
        raise RuntimeError("DATABASE_URL not set")

    last_err = None
    for idx, url in enumerate(urls, start=1):
        for attempt, delay in enumerate([0.2, 0.4, 0.8], start=1):
            try:
                eng = create_engine(url, pool_pre_ping=True, pool_recycle=180)
                # quick probe with a very short connection to validate reachability
                with eng.connect() as _:
                    pass
                return eng
            except OperationalError as e:
                last_err = e
                time.sleep(delay)
                continue
    # If all candidates failed, raise the last error
    raise last_err or RuntimeError("No working DB URL")

def _read_sql(sql, params=None):
    eng = _engine()
    with eng.connect() as conn:
        return pd.read_sql(sql, conn, params=params)

# ---------------------------------------------------------------------
# psycopg2 connection for WRITES/EXEC
# ---------------------------------------------------------------------
def _current_pg_env():
    return {
        "DATABASE_URL": (os.getenv("DATABASE_URL") or "").strip(),
        "PGHOST": os.getenv("PGHOST"),
        "PGDATABASE": os.getenv("PGDATABASE", "postgres"),
        "PGUSER": os.getenv("PGUSER", "postgres"),
        "PGPASSWORD": os.getenv("PGPASSWORD", ""),
        "PGPORT": int(os.getenv("PGPORT", "5432")),   # direct Postgres
        "PGSSLMODE": os.getenv("PGSSLMODE", "require"),
    }

@contextmanager
def _conn():
    """
    Open a PostgreSQL connection for write ops.
    Uses DATABASE_URL if present; else discrete PG* vars.
    When unreachable, raises a friendly RuntimeError for UI to show "Write disabled in offline mode".
    """
    env = _current_pg_env()
    try:
        if env["DATABASE_URL"]:
            conn = psycopg2.connect(
                env["DATABASE_URL"],
                cursor_factory=RealDictCursor,
                connect_timeout=10,
            )
        else:
            if not env["PGHOST"]:
                raise RuntimeError("PGHOST is not set (and no DATABASE_URL).")
            conn = psycopg2.connect(
                host=env["PGHOST"],
                dbname=env["PGDATABASE"],
                user=env["PGUSER"],
                password=env["PGPASSWORD"],
                port=env["PGPORT"],
                sslmode=env["PGSSLMODE"],
                cursor_factory=RealDictCursor,
                connect_timeout=10,
            )
        # optional: enable autocommit to avoid lingering transactions on simple writes
        conn.autocommit = True
        yield conn
    except Exception as e:
        # Convert any connection error into a single friendly message for the UI layer
        raise RuntimeError("Write disabled in offline mode") from e
    finally:
        try:
            conn.close()
        except Exception:
            pass

def _exec(sql, params=None, return_df=False):
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or {})
            if return_df:
                rows = cur.fetchall()
                return pd.DataFrame(rows)
        # autocommit=True above; explicit commit only if you later disable it
        # conn.commit()

# ---------------------------------------------------------------------
# READ helpers
# ---------------------------------------------------------------------
def fetch_contributions():
    cols = ["house_no","family_name","lane","rate_category","email",
            "cumulative_debt_prior","jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec",
            "ytd","current_debt","status","remarks","updated_at"]
    present = _read_sql("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema='public' AND table_name='contributions'
    """)
    keep = [c for c in cols if c in present["column_name"].tolist()]
    if not keep:
        # table doesn't exist / no columns yet
        return pd.DataFrame(columns=[
            "House No","Family Name","Lane","Rate Category","Email",
            "Cumulative Debt (2024 & Prior)","JAN","FEB","MAR","APR","MAY","JUN","JUL",
            "AUG","SEP","OCT","NOV","DEC","YTD","Current Debt","Status","Remarks"
        ])

    q = f"SELECT {', '.join(keep)} FROM public.contributions ORDER BY updated_at DESC NULLS LAST, family_name ASC"
    df = _read_sql(q)
    rename = {
        "house_no":"House No","family_name":"Family Name","lane":"Lane","rate_category":"Rate Category",
        "email":"Email","cumulative_debt_prior":"Cumulative Debt (2024 & Prior)",
        "jan":"JAN","feb":"FEB","mar":"MAR","apr":"APR","may":"MAY","jun":"JUN","jul":"JUL",
        "aug":"AUG","sep":"SEP","oct":"OCT","nov":"NOV","dec":"DEC",
        "ytd":"YTD","current_debt":"Current Debt","status":"Status","remarks":"Remarks"
    }
    return df.rename(columns={k: v for k, v in rename.items() if k in df.columns})

def fetch_expenses():
    df = _read_sql("""
        SELECT id, date, description, category, vendor, phone, amount_kes, mode, remarks
        FROM public.expenses
        ORDER BY date DESC, id DESC
    """)
    return df.rename(columns={
        "date":"Date","description":"Description","category":"Category","vendor":"Vendor",
        "phone":"Phone","amount_kes":"Amount (KES)","mode":"Mode","remarks":"Remarks"
    })

def fetch_rates():
    df = _read_sql("SELECT rate_category, amount FROM public.rates ORDER BY rate_category")
    return df.rename(columns={"rate_category":"Rate Category","amount":"Amount"})

def fetch_expense_requests():
    df = _read_sql("""
        SELECT id, date, description, category, requested_by, amount_kes, status, remarks
        FROM public.expense_requests
        ORDER BY date DESC, id DESC
    """)
    return df.rename(columns={
        "date":"Date","description":"Description","category":"Category","requested_by":"Requested By",
        "amount_kes":"Amount (KES)","status":"Status","remarks":"Remarks"
    })

def fetch_contribution_requests():
    df = _read_sql("""
        SELECT id, date, month, family_name, house_no, lane, rate_category, amount_kes, status, remarks
        FROM public.contribution_requests
        ORDER BY date DESC, id DESC
    """)
    return df.rename(columns={
        "date":"Date","month":"Month","family_name":"Family Name","house_no":"House No","lane":"Lane",
        "rate_category":"Rate Category","amount_kes":"Amount (KES)","status":"Status","remarks":"Remarks"
    })

def fetch_special():
    df = _read_sql("""
        SELECT id, event, date, type, contributors, amount, remarks
        FROM public.special
        ORDER BY date DESC, id DESC
    """)
    return df.rename(columns={
        "event":"Event","date":"Date","type":"Type","contributors":"Contributors",
        "amount":"Amount","remarks":"Remarks"
    })

def fetch_special_requests():
    df = _read_sql("""
        SELECT id, date, event, type, requested_by, amount, status, remarks
        FROM public.special_requests
        ORDER BY date DESC, id DESC
    """)
    return df.rename(columns={
        "date":"Date","requested_by":"Requested By",
        "amount":"Amount","status":"Status","event":"Event","type":"Type"
    })

def fetch_cash_management():
    df = _read_sql("""
        SELECT cash_balance_cd, cash_withdrawal
        FROM public.cash_management
        ORDER BY updated_at DESC NULLS LAST
        LIMIT 1
    """)
    if df.empty:
        return pd.DataFrame({"Cash Balance c/d":[0],"Cash Withdrawal":[0]})
    return df.rename(columns={
        "cash_balance_cd":"Cash Balance c/d",
        "cash_withdrawal":"Cash Withdrawal"
    })

def load_all():
    return {
        "contributions": fetch_contributions(),
        "expenses": fetch_expenses(),
        "special": fetch_special(),
        "rates": fetch_rates(),
        "expense_requests": fetch_expense_requests(),
        "contribution_requests": fetch_contribution_requests(),
        "special_requests": fetch_special_requests(),
        "cash_management": fetch_cash_management(),
    }

# ---------------------------------------------------------------------
# WRITE helpers
# ---------------------------------------------------------------------
def insert_contribution_request(date, month, family_name, house_no, lane, rate_category, amount_kes, remarks):
    _exec("""
        INSERT INTO public.contribution_requests
            (date, month, family_name, house_no, lane, rate_category, amount_kes, status, remarks)
        VALUES (%(date)s, %(month)s, %(family_name)s, %(house_no)s, %(lane)s, %(rate_category)s, %(amount_kes)s,
                'Pending Approval', %(remarks)s)
    """, {
        "date": date, "month": month, "family_name": family_name, "house_no": house_no,
        "lane": lane, "rate_category": rate_category, "amount_kes": amount_kes, "remarks": remarks
    })

def approve_contribution_request(req_row, action, approval_remarks, current_month):
    rid = req_row["id"] if "id" in req_row else req_row.name
    _exec("""
        UPDATE public.contribution_requests
           SET status = %(status)s, remarks = CONCAT(%(remarks)s, ' | ', COALESCE(remarks,'')) 
         WHERE id = %(id)s
    """, {"status": action, "remarks": approval_remarks, "id": rid})

    if action != "Approve":
        return

    month_col = current_month.lower()  # 'JAN' -> 'jan'
    _exec(f"""
        UPDATE public.contributions
           SET {month_col} = %(amount)s,
               ytd = COALESCE(jan,0)+COALESCE(feb,0)+COALESCE(mar,0)+COALESCE(apr,0)+COALESCE(may,0)+COALESCE(jun,0)
                   +COALESCE(jul,0)+COALESCE(aug,0)+COALESCE(sep,0)+COALESCE(oct,0)+COALESCE(nov,0)+COALESCE(dec,0),
               updated_at = NOW()
         WHERE family_name = %(family_name)s
    """, {
        "amount": req_row.get("Amount (KES)", req_row.get("amount_kes")),
        "family_name": req_row.get("Family Name", req_row.get("family_name")),
    })

def insert_expense(date, description, category, vendor, phone, amount_kes, mode, remarks):
    _exec("""
        INSERT INTO public.expenses
            (date, description, category, vendor, phone, amount_kes, mode, remarks)
        VALUES (%(date)s, %(description)s, %(category)s, %(vendor)s, %(phone)s, %(amount_kes)s, %(mode)s, %(remarks)s)
    """, {
        "date": date, "description": description, "category": category, "vendor": vendor,
        "phone": phone, "amount_kes": amount_kes, "mode": mode, "remarks": remarks
    })

def insert_expense_request(date, description, category, requested_by, amount_kes, remarks):
    _exec("""
        INSERT INTO public.expense_requests
            (date, description, category, requested_by, amount_kes, status, remarks)
        VALUES (%(date)s, %(description)s, %(category)s, %(requested_by)s, %(amount_kes)s,
                'Pending Approval', %(remarks)s)
    """, {
        "date": date, "description": description, "category": category,
        "requested_by": requested_by, "amount_kes": amount_kes, "remarks": remarks
    })

def set_expense_request_status(request_id, status, approval_remarks):
    _exec("""
        UPDATE public.expense_requests
           SET status = %(status)s, remarks = CONCAT(%(remarks)s, ' | ', COALESCE(remarks,'')) 
         WHERE id = %(id)s
    """, {"status": status, "remarks": approval_remarks, "id": request_id})

def update_cash_management(cash_balance_cd, cash_withdrawal):
    _exec("""
        INSERT INTO public.cash_management (cash_balance_cd, cash_withdrawal, updated_at)
        VALUES (%(a)s, %(b)s, NOW())
    """, {"a": cash_balance_cd, "b": cash_withdrawal})

def upsert_rates(df_rates):
    for _, r in df_rates.iterrows():
        _exec("""
            INSERT INTO public.rates (rate_category, amount)
            VALUES (%(cat)s, %(amt)s)
            ON CONFLICT (rate_category) DO UPDATE SET amount = EXCLUDED.amount
        """, {"cat": r["Rate Category"], "amt": float(r["Amount"])})

def update_household_rate_email(df_households):
    for _, r in df_households.iterrows():
        _exec("""
            UPDATE public.contributions
               SET rate_category = %(rc)s, email = %(em)s
             WHERE house_no = %(hn)s
        """, {"rc": r["Rate Category"], "em": r.get("Email",""), "hn": str(r["House No"])})

# ---------------------------------------------------------------------
# Special contributions (schema guard + writes)
# ---------------------------------------------------------------------
def insert_special_request(date, event, type, requested_by, amount, remarks):
    _ensure_special_tables()
    _exec("""
        INSERT INTO public.special_requests
            (date, event, type, requested_by, amount, status, remarks)
        VALUES (%(date)s, %(event)s, %(type)s, %(requested_by)s, %(amount)s, 'Pending Approval', %(remarks)s)
    """, {
        "date": date, "event": event, "type": type,
        "requested_by": requested_by, "amount": amount, "remarks": remarks
    })

def set_special_request_status(request_id, status, approval_remarks):
    _ensure_special_tables()
    _exec("""
        UPDATE public.special_requests
           SET status = %(status)s,
               remarks = CONCAT(%(remarks)s, ' | ', COALESCE(remarks,''))
         WHERE id = %(id)s
    """, {"status": status, "remarks": approval_remarks, "id": int(request_id)})

def insert_special(date, event, type, contributors, amount, remarks):
    _ensure_special_tables()
    _exec("""
        INSERT INTO public.special
            (date, event, type, contributors, amount, remarks)
        VALUES (%(date)s, %(event)s, %(type)s, %(contributors)s, %(amount)s, %(remarks)s)
    """, {
        "date": date, "event": event, "type": type,
        "contributors": contributors, "amount": amount, "remarks": remarks
    })

def _ensure_special_tables():
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS public.special (
                    id SERIAL PRIMARY KEY,
                    date DATE NOT NULL,
                    event TEXT NOT NULL,
                    type TEXT NOT NULL,
                    contributors TEXT,
                    amount NUMERIC(14,2) NOT NULL DEFAULT 0,
                    remarks TEXT
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS public.special_requests (
                    id SERIAL PRIMARY KEY,
                    date DATE NOT NULL,
                    event TEXT NOT NULL,
                    type TEXT NOT NULL,
                    requested_by TEXT NOT NULL,
                    amount NUMERIC(14,2) NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'Pending Approval',
                    remarks TEXT
                );
            """)
        # conn.autocommit=True already; explicit commit not required
