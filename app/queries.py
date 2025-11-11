# app/queries.py
from typing import List, Optional, Dict, Any, Tuple
from app.db import get_conn

# ---------- READS ----------
def list_contributions(search: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
    sql = """
        SELECT house_no, family_name, lane, rate_category, email,
               cumulative_debt, jan,feb,mar,apr,may,jun,jul,aug,sep,oct,nov,dec,
               ytd, current_debt, remarks, updated_at
        FROM contributions
    """
    params: Tuple = ()
    if search:
        sql += " WHERE house_no ILIKE %s OR family_name ILIKE %s OR lane ILIKE %s"
        params = (f"%{search}%", f"%{search}%", f"%{search}%")
    sql += " ORDER BY house_no LIMIT %s"
    params = params + (limit,)

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

def list_expenses(limit: int = 200) -> List[Dict[str, Any]]:
    sql = """
        SELECT date, description, category, vendor, phone, amount_kes,
               mode, remarks, receipt, created_at
        FROM expenses
        ORDER BY date DESC, created_at DESC
        LIMIT %s
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (limit,))
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

def get_summary_totals() -> Dict[str, Any]:
    """
    Example: total expenses, total current debt, cash management snapshot.
    """
    out = {}
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT COALESCE(SUM(amount_kes),0) FROM expenses;")
        out["total_expenses_kes"] = float(cur.fetchone()[0] or 0)

        cur.execute("SELECT COALESCE(SUM(current_debt),0) FROM contributions;")
        out["total_current_debt_kes"] = float(cur.fetchone()[0] or 0)

        cur.execute("SELECT cash_balance_cd, cash_withdrawal FROM cash_management WHERE id=1;")
        row = cur.fetchone()
        if row:
            out["cash_balance_cd"] = float(row[0] or 0)
            out["cash_withdrawal"] = float(row[1] or 0)
        else:
            out["cash_balance_cd"] = 0.0
            out["cash_withdrawal"] = 0.0
    return out

# ---------- WRITES ----------
def upsert_contribution(
    house_no: str,
    family_name: Optional[str] = None,
    lane: Optional[str] = None,
    rate_category: Optional[str] = None,
    email: Optional[str] = None,
    jan: float = 0, feb: float = 0, mar: float = 0, apr: float = 0, may: float = 0, jun: float = 0,
    jul: float = 0, aug: float = 0, sep: float = 0, oct: float = 0, nov: float = 0, dec: float = 0,
    cumulative_debt: float = 0, ytd: float = 0, current_debt: float = 0, remarks: Optional[str] = None
) -> None:
    sql = """
        INSERT INTO contributions(
          house_no, family_name, lane, rate_category, email,
          cumulative_debt, jan, feb, mar, apr, may, jun, jul, aug, sep, oct, nov, dec,
          ytd, current_debt, remarks, updated_at
        ) VALUES (
          %(house_no)s, %(family_name)s, %(lane)s, %(rate_category)s, %(email)s,
          %(cumulative_debt)s, %(jan)s, %(feb)s, %(mar)s, %(apr)s, %(may)s, %(jun)s, %(jul)s, %(aug)s, %(sep)s, %(oct)s, %(nov)s, %(dec)s,
          %(ytd)s, %(current_debt)s, %(remarks)s, NOW()
        )
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
          updated_at    = NOW();
    """
    params = dict(
        house_no=house_no, family_name=family_name, lane=lane, rate_category=rate_category, email=email,
        cumulative_debt=cumulative_debt, jan=jan, feb=feb, mar=mar, apr=apr, may=may, jun=jun,
        jul=jul, aug=aug, sep=sep, oct=oct, nov=nov, dec=dec,
        ytd=ytd, current_debt=current_debt, remarks=remarks
    )
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        conn.commit()

def add_expense(
    date_iso: str, description: str, category: str,
    vendor: str, phone: str, amount_kes: float,
    mode: str, remarks: str = "", receipt: str | None = None
) -> None:
    sql = """
        INSERT INTO expenses("date", description, category, vendor, phone, amount_kes, mode, remarks, receipt)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (date_iso, description, category, vendor, phone, amount_kes, mode, remarks, receipt))
        conn.commit()
