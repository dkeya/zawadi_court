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
