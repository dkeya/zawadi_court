# streamlit_app.py
import os
import datetime as dt
import pandas as pd
import streamlit as st
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv, find_dotenv

# ---------- Env & DB ----------
load_dotenv(find_dotenv(filename=".env", usecwd=True), override=True)

DSN = dict(
    host=os.getenv("PGHOST"),
    dbname=os.getenv("PGDATABASE"),
    user=os.getenv("PGUSER"),
    password=os.getenv("PGPASSWORD"),
    port=os.getenv("PGPORT", "5432"),
    sslmode=os.getenv("PGSSLMODE", "require"),
)

def fetch_df(sql, params=None):
    with psycopg2.connect(**DSN) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params or ())
            rows = cur.fetchall()
    return pd.DataFrame(rows)

def execute(sql, params=None):
    with psycopg2.connect(**DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
        conn.commit()

# ---------- UI ----------
st.set_page_config(page_title="Zawadi Court — Treasurer", page_icon="🏠", layout="wide")
st.title("🏠 Zawadi Court — Treasurer Console")

# Totals
totals = fetch_df("""
    select
      (select coalesce(sum(amount_kes),0) from expenses)              as total_expenses_kes,
      (select coalesce(sum(current_debt),0) from contributions)       as total_current_debt_kes,
      (select coalesce(max(cash_balance_cd),0) from cash_management)  as cash_balance_cd,
      (select coalesce(max(cash_withdrawal),0)  from cash_management) as cash_withdrawal
""")

c1, c2, c3, c4 = st.columns(4)
if not totals.empty:
    c1.metric("Total expenses (KES)", f"{totals.loc[0,'total_expenses_kes']:,.0f}")
    c2.metric("Current debt (KES)", f"{totals.loc[0,'total_current_debt_kes']:,.0f}")
    c3.metric("Cash balance c/d (KES)", f"{totals.loc[0,'cash_balance_cd']:,.0f}")
    c4.metric("Cash withdrawal (KES)", f"{totals.loc[0,'cash_withdrawal']:,.0f}")

st.divider()

tab1, tab2 = st.tabs(["📜 Recent records", "➕ Add expense"])

with tab1:
    colA, colB = st.columns(2)
    # Recent expenses
    df_exp = fetch_df("""
        select id, "date", description, category, vendor, amount_kes, mode, remarks
        from expenses
        order by "date" desc, id desc
        limit 20
    """)
    df_con = fetch_df("""
        select house_no, family_name, lane, rate_category,
               ytd, current_debt, jan, feb, mar, apr, may, jun,
               jul, aug, sep, oct, nov, dec, updated_at
        from contributions
        order by updated_at desc nulls last
        limit 20
    """)
    with colA:
        st.subheader("Recent expenses")
        st.dataframe(df_exp, use_container_width=True, hide_index=True)
    with colB:
        st.subheader("Recent contributions (top 20 by update)")
        st.dataframe(df_con, use_container_width=True, hide_index=True)

with tab2:
    st.subheader("Record a new expense")
    with st.form("expense_form", clear_on_submit=True):
        col1, col2 = st.columns(2)
        with col1:
            date_val = st.date_input("Date", value=dt.date.today())
            description = st.text_input("Description")
            category = st.text_input("Category")
            vendor = st.text_input("Vendor")
        with col2:
            phone = st.text_input("Phone")
            amount = st.number_input("Amount (KES)", min_value=0.0, step=100.0, format="%.2f")
            mode = st.text_input("Mode (e.g., M-Pesa, Cash, Bank)")
            remarks = st.text_input("Remarks")
        submitted = st.form_submit_button("Save expense")
        if submitted:
            if not description or amount <= 0:
                st.error("Please provide at least a description and a positive amount.")
            else:
                try:
                    execute("""
                        insert into expenses("date", description, category, vendor, phone, amount_kes, mode, remarks)
                        values (%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (date_val, description.strip(), category.strip(), vendor.strip(),
                          phone.strip(), float(amount), mode.strip(), remarks.strip()))
                    st.success("Expense saved.")
                except Exception as e:
                    st.error(f"Failed to save expense: {e}")
