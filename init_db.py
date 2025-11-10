# init_db.py
import os, psycopg2
from psycopg2.extras import RealDictCursor

DDL = """
-- Core ledger of monthly contributions
CREATE TABLE IF NOT EXISTS public.contributions (
  house_no               TEXT PRIMARY KEY,
  family_name            TEXT NOT NULL,
  lane                   TEXT,
  rate_category          TEXT,
  email                  TEXT,
  cumulative_debt_prior  NUMERIC(14,2) DEFAULT 0,
  jan NUMERIC(14,2), feb NUMERIC(14,2), mar NUMERIC(14,2),
  apr NUMERIC(14,2), may NUMERIC(14,2), jun NUMERIC(14,2),
  jul NUMERIC(14,2), aug NUMERIC(14,2), sep NUMERIC(14,2),
  oct NUMERIC(14,2), nov NUMERIC(14,2), "dec" NUMERIC(14,2),
  ytd           NUMERIC(14,2) DEFAULT 0,
  current_debt  NUMERIC(14,2) DEFAULT 0,
  status        TEXT,
  remarks       TEXT,
  updated_at    TIMESTAMP DEFAULT NOW()
);

-- Approved / rejected requests to update contributions
CREATE TABLE IF NOT EXISTS public.contribution_requests (
  id SERIAL PRIMARY KEY,
  date DATE NOT NULL,
  month TEXT NOT NULL,
  family_name TEXT NOT NULL,
  house_no TEXT NOT NULL,
  lane TEXT,
  rate_category TEXT,
  amount_kes NUMERIC(14,2) NOT NULL,
  status TEXT NOT NULL DEFAULT 'Pending Approval',
  remarks TEXT
);

-- Expenses ledger
CREATE TABLE IF NOT EXISTS public.expenses (
  id SERIAL PRIMARY KEY,
  date DATE NOT NULL,
  description TEXT NOT NULL,
  category TEXT,
  vendor TEXT,
  phone TEXT,
  amount_kes NUMERIC(14,2) NOT NULL,
  mode TEXT,
  remarks TEXT
);

-- Expense requests (for approval)
CREATE TABLE IF NOT EXISTS public.expense_requests (
  id SERIAL PRIMARY KEY,
  date DATE NOT NULL,
  description TEXT NOT NULL,
  category TEXT,
  requested_by TEXT NOT NULL,
  amount_kes NUMERIC(14,2) NOT NULL,
  status TEXT NOT NULL DEFAULT 'Pending Approval',
  remarks TEXT
);

-- Rate catalog
CREATE TABLE IF NOT EXISTS public.rates (
  rate_category TEXT PRIMARY KEY,
  amount NUMERIC(14,2) NOT NULL
);

-- Cash mgmt snapshot
CREATE TABLE IF NOT EXISTS public.cash_management (
  id SERIAL PRIMARY KEY,
  cash_balance_cd NUMERIC(14,2) NOT NULL DEFAULT 0,
  cash_withdrawal NUMERIC(14,2) NOT NULL DEFAULT 0,
  updated_at TIMESTAMP DEFAULT NOW()
);

-- Specials (your write helpers already guard these, but we add here for completeness)
CREATE TABLE IF NOT EXISTS public.special (
  id SERIAL PRIMARY KEY,
  date DATE NOT NULL,
  event TEXT NOT NULL,
  type TEXT NOT NULL,
  contributors TEXT,
  amount NUMERIC(14,2) NOT NULL DEFAULT 0,
  remarks TEXT
);
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
"""

SEED = """
INSERT INTO public.rates (rate_category, amount) VALUES
  ('Standard', 1000),
  ('Premium', 1500)
ON CONFLICT (rate_category) DO UPDATE SET amount = EXCLUDED.amount;

INSERT INTO public.contributions
  (house_no, family_name, lane, rate_category, email, cumulative_debt_prior, status, remarks)
VALUES
  ('H-01', 'Achieng', 'Lane 1', 'Standard', 'achieng@example.com', 0, 'Active', ''),
  ('H-02', 'Kiptoo',  'Lane 2', 'Premium',  'kiptoo@example.com',  0, 'Active', '')
ON CONFLICT (house_no) DO NOTHING;
"""

def main():
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL not set")
    with psycopg2.connect(url, cursor_factory=RealDictCursor) as conn:
        with conn.cursor() as cur:
            cur.execute(DDL)
            cur.execute(SEED)
        conn.commit()
    print("✅ Tables ensured and seed data inserted.")

if __name__ == "__main__":
    main()
