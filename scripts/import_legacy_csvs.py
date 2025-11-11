# scripts/import_legacy_csvs.py
import os
import sys
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch, execute_values
from dotenv import load_dotenv, find_dotenv
from pathlib import Path

# ---------- numeric + date helpers you already had ----------
def f(x):
    """Coerce to float; invalid -> 0."""
    try:
        if pd.isna(x): 
            return 0
        s = str(x).strip().replace(",", "")
        if s in ("", "-", " - "):
            return 0
        return float(s)
    except Exception:
        return 0

def to_date(s):
    """Coerce to Python date (for DATE columns) or None."""
    try:
        ts = pd.to_datetime(s, dayfirst=True, errors="coerce")
        if pd.isna(ts):
            return None
        return ts.date()
    except Exception:
        return None

# ---------- NEW helpers (near top) ----------
def to_num(x):
    """Coerce to float; invalid -> 0."""
    try:
        if pd.isna(x):
            return 0.0
        s = str(x).replace(",", "").strip()
        return float(s) if s not in ("", "-", " - ") else 0.0
    except Exception:
        return 0.0

def to_dt(x):
    """Coerce to Python datetime (for TIMESTAMP) or None if invalid/blank."""
    if pd.isna(x):
        return None
    s = str(x).strip()
    if s == "" or s.lower() == "nat":
        return None
    ts = pd.to_datetime(s, errors="coerce", dayfirst=True)
    if pd.isna(ts):
        return None
    # Return full datetime for TIMESTAMP columns; use .date() if your column is DATE
    return ts.to_pydatetime()

def to_text(x):
    """Normalize text; blanks -> '' (or use None if you prefer NULLs)."""
    return "" if pd.isna(x) else str(x).strip()

# ---------- load env ----------
env_path = find_dotenv(filename=".env", usecwd=True)
load_dotenv(env_path, override=True)

dsn = dict(
    host=os.getenv("PGHOST"),
    dbname=os.getenv("PGDATABASE"),
    user=os.getenv("PGUSER"),
    password=os.getenv("PGPASSWORD"),
    port=os.getenv("PGPORT", "5432"),
    sslmode=os.getenv("PGSSLMODE", "require"),
)

legacy_dir = Path("data/legacy")
if not legacy_dir.exists():
    sys.exit("No data/legacy folder found.")

print("Connecting…")
with psycopg2.connect(**dsn) as conn:
    cur = conn.cursor()

    # ---- rates.csv (must go first)
    p = legacy_dir / "rates.csv"
    if p.exists():
        df = pd.read_csv(p)
        df.columns = [c.strip() for c in df.columns]
        if {"Rate Category","Amount"}.issubset(df.columns):
            rows = [(str(r["Rate Category"]).strip(), f(r["Amount"])) for _, r in df.iterrows()]
            if rows:
                execute_batch(
                    cur,
                    """
                    insert into rates(rate_category, amount) values (%s,%s)
                    on conflict (rate_category) do update set amount=excluded.amount
                    """,
                    rows, page_size=100
                )
            print(f"✓ rates: {len(rows)} rows")

    # ---- contributions.csv
    p = legacy_dir / "contributions.csv"
    if p.exists():
        df = pd.read_csv(p, dtype=str)
        df.columns = [c.strip() for c in df.columns]

        months = ["JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"]
        # ensure month cols exist and are numeric
        for m in months:
            if m in df.columns:
                df[m] = df[m].apply(f)
            else:
                df[m] = 0

        # numeric helpers
        df["Cumulative Debt (2024 & Prior)"] = df.get("Cumulative Debt (2024 & Prior)", 0).apply(f)
        df["YTD"]          = df.get("YTD", 0).apply(f)
        df["Current Debt"] = df.get("Current Debt", 0).apply(f)

        # build rows, skipping records without a House No (needed for ON CONFLICT)
        rows = []
        for _, r in df.iterrows():
            house_no = str(r.get("House No","")).strip()
            if not house_no:
                continue  # skip invalid key

            rows.append((
                house_no,
                str(r.get("Family Name","")).strip() or None,
                str(r.get("Lane","")).strip() or None,
                str(r.get("Rate Category","")).strip() or None,
                str(r.get("Email","")).strip() or None,
                r["Cumulative Debt (2024 & Prior)"],
                r["JAN"], r["FEB"], r["MAR"], r["APR"], r["MAY"], r["JUN"],
                r["JUL"], r["AUG"], r["SEP"], r["OCT"], r["NOV"], r["DEC"],
                r["YTD"], r["Current Debt"],
                (str(r.get("Remarks","")).strip() or None),
            ))

        if rows:
            sql = """
                INSERT INTO contributions(
                  house_no,family_name,lane,rate_category,email,
                  cumulative_debt,jan,feb,mar,apr,may,jun,jul,aug,sep,oct,nov,dec,
                  ytd,current_debt,remarks
                ) VALUES %s
                ON CONFLICT (house_no) DO UPDATE SET
                  family_name   = EXCLUDED.family_name,
                  lane          = EXCLUDED.lane,
                  rate_category = EXCLUDED.rate_category,
                  email         = EXCLUDED.email,
                  cumulative_debt = EXCLUDED.cumulative_debt,
                  jan=EXCLUDED.jan, feb=EXCLUDED.feb, mar=EXCLUDED.mar, apr=EXCLUDED.apr, may=EXCLUDED.may, jun=EXCLUDED.jun,
                  jul=EXCLUDED.jul, aug=EXCLUDED.aug, sep=EXCLUDED.sep, oct=EXCLUDED.oct, nov=EXCLUDED.nov, dec=EXCLUDED.dec,
                  ytd           = EXCLUDED.ytd,
                  current_debt  = EXCLUDED.current_debt,
                  remarks       = EXCLUDED.remarks,
                  updated_at    = now();
            """
            execute_values(cur, sql, rows, page_size=1000)
            print(f"✓ contributions: {len(rows)} rows")
        else:
            print("✓ contributions: 0 rows (no valid House No)")

    # ---- expenses.csv (DATE column expected)
    p = legacy_dir / "expenses.csv"
    if p.exists():
        df = pd.read_csv(p)
        df.columns = [c.strip() for c in df.columns]
        rows = []
        for _, r in df.iterrows():
            rows.append((
                to_date(r.get("Date")),
                str(r.get("Description") or ""),
                str(r.get("Category") or ""),
                str(r.get("Vendor") or ""),
                str(r.get("Phone") or ""),
                f(r.get("Amount (KES)")),
                str(r.get("Mode") or ""),
                str(r.get("Remarks") or ""),
                str(r.get("Receipt")) if not pd.isna(r.get("Receipt")) else None,
            ))
        if rows:
            execute_batch(cur, """
                insert into expenses("date",description,category,vendor,phone,amount_kes,mode,remarks,receipt)
                values (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, rows, page_size=1000)
        print(f"✓ expenses: {len(rows)} rows")

    # ---- special.csv (DATE column expected)
    p = legacy_dir / "special.csv"
    if p.exists():
        df = pd.read_csv(p)
        rows = []
        for _, r in df.iterrows():
            rows.append((
                str(r.get("Event") or ""),
                to_date(r.get("Date")),
                str(r.get("Type") or ""),
                str(r.get("Contributors") or ""),
                f(r.get("Amount")),
                str(r.get("Remarks") or ""),
            ))
        if rows:
            execute_batch(cur, """
                insert into special(event,"date","type",contributors,amount,remarks)
                values (%s,%s,%s,%s,%s,%s)
            """, rows, page_size=1000)
        print(f"✓ special: {len(rows)} rows")

    # ---- requests (TIMESTAMP advised). REPLACED with your integrated block
    def import_req(csv_name, table, cols):
        p = legacy_dir / csv_name
        if not p.exists():
            print(f"✓ {table}: 0 rows (file not found)")
            return
        df = pd.read_csv(p, dtype=str)
        df.columns = [c.strip() for c in df.columns]

        rows = []
        for _, r in df.iterrows():
            # Map canonical CSV headers:
            # Date, Description, Category, Requested By, Amount (KES), Status, Remarks
            # Handle possible header typo for Amount (KES)
            amt = None
            if "Amount (KES)" in df.columns:
                amt = to_num(r.get("Amount (KES)"))
            elif "Amount (KES" in df.columns:
                amt = to_num(r.get("Amount (KES"))
            else:
                amt = to_num(r.get("Amount"))

            row = (
                to_dt(r.get("Date")),                 # TIMESTAMP or None
                to_text(r.get("Description")),
                to_text(r.get("Category")),
                to_text(r.get("Requested By")),
                amt,
                to_text(r.get("Status")),
                to_text(r.get("Remarks")),
            )
            rows.append(row)

        if not rows:
            print(f"✓ {table}: 0 rows")
            return

        sql = f"""
            INSERT INTO {table} ({','.join(cols)})
            VALUES %s
            ON CONFLICT DO NOTHING;
        """
        execute_values(cur, sql, rows, page_size=1000)
        print(f"✓ {table}: {len(rows)} rows")

    # expense_requests
    import_req(
        "expense_requests.csv",
        "expense_requests",
        ["date","description","category","requested_by","amount_kes","status","remarks"]
    )

    # contribution_requests
    # CSV headers expected: Date, Month, Family Name, House No, Lane, Rate Category, Amount (KES), Status, Remarks
    p = legacy_dir / "contribution_requests.csv"
    if p.exists():
        df = pd.read_csv(p, dtype=str)
        df.columns = [c.strip() for c in df.columns]
        rows = []
        for _, r in df.iterrows():
            # amount handling like above
            if "Amount (KES)" in df.columns:
                amt = to_num(r.get("Amount (KES)"))
            elif "Amount (KES" in df.columns:
                amt = to_num(r.get("Amount (KES"))
            else:
                amt = to_num(r.get("Amount"))
            rows.append((
                to_dt(r.get("Date")),
                to_text(r.get("Month")),
                to_text(r.get("Family Name")),
                to_text(r.get("House No")),
                to_text(r.get("Lane")),
                to_text(r.get("Rate Category")),
                amt,
                to_text(r.get("Status")),
                to_text(r.get("Remarks")),
            ))
        if rows:
            execute_values(
                cur,
                """
                INSERT INTO contribution_requests
                  (date,month,family_name,house_no,lane,rate_category,amount_kes,status,remarks)
                VALUES %s
                ON CONFLICT DO NOTHING;
                """,
                rows, page_size=1000
            )
            print(f"✓ contribution_requests: {len(rows)} rows")
        else:
            print("✓ contribution_requests: 0 rows")
    else:
        print("✓ contribution_requests: 0 rows (file not found)")

    # special_requests
    # CSV headers expected: Date, Event, Type, Requested By, Amount, Status, Remarks
    p = legacy_dir / "special_requests.csv"
    if p.exists():
        df = pd.read_csv(p, dtype=str)
        df.columns = [c.strip() for c in df.columns]
        rows = []
        for _, r in df.iterrows():
            rows.append((
                to_dt(r.get("Date")),
                to_text(r.get("Event")),
                to_text(r.get("Type")),
                to_text(r.get("Requested By")),
                to_num(r.get("Amount")),
                to_text(r.get("Status")),
                to_text(r.get("Remarks")),
            ))
        if rows:
            execute_values(
                cur,
                """
                INSERT INTO special_requests
                  (date,event,type,requested_by,amount,status,remarks)
                VALUES %s
                ON CONFLICT DO NOTHING;
                """,
                rows, page_size=1000
            )
            print(f"✓ special_requests: {len(rows)} rows")
        else:
            print("✓ special_requests: 0 rows")
    else:
        print("✓ special_requests: 0 rows (file not found)")

    # ---- cash_management.csv (single-row)
    p = legacy_dir / "cash_management.csv"
    if p.exists():
        df = pd.read_csv(p)
        if not df.empty:
            r = df.iloc[0]
            cur.execute(
                """
                insert into cash_management(id, cash_balance_cd, cash_withdrawal)
                values (1, %s, %s)
                on conflict (id) do update set
                  cash_balance_cd = excluded.cash_balance_cd,
                  cash_withdrawal = excluded.cash_withdrawal,
                  updated_at = now()
                """,
                (f(r.get("Cash Balance c/d")), f(r.get("Cash Withdrawal")))
            )
            print("✓ cash_management: upserted")

    conn.commit()
print("✅ Import complete.")
