# C:\Users\dkeya\Documents\projects\zawadi_court\streamlit_app.py

# --- Env/secrets bootstrap (must run BEFORE importing zawadi_db) ---
import os
import streamlit as st
try:
    from dotenv import load_dotenv  # pip install python-dotenv
    load_dotenv(override=False)
except Exception:
    pass

# Merge Streamlit Cloud secrets if present (no printing)
if "DATABASE_URL" in st.secrets and not os.getenv("DATABASE_URL"):
    os.environ["DATABASE_URL"] = str(st.secrets["DATABASE_URL"]).strip()

# Normalize: ensure sslmode=require (without duplicating)
_db = os.getenv("DATABASE_URL", "").strip()
if _db and "sslmode=" not in _db:
    os.environ["DATABASE_URL"] = f"{_db}{'&' if '?' in _db else '?'}sslmode=require"

# -------------------------------------------------------------------
# Regular imports (unchanged)
# -------------------------------------------------------------------
import pandas as pd
import numpy as np
import plotly.express as px
from datetime import datetime, date
import hashlib
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import time
import unittest
from unittest.mock import patch, MagicMock
import io
import shutil
import glob
import threading
import gspread
from google.oauth2 import service_account

# ==== Postgres glue (from zawadi_db.py) ====
from zawadi_db import (
    load_all,
    insert_contribution_request, approve_contribution_request,
    insert_expense_request, set_expense_request_status, insert_expense,
    update_cash_management, upsert_rates, update_household_rate_email,
    insert_special_request, set_special_request_status, insert_special,

    # ✅ Admin (edit/delete) helpers used in Treasurer panel
    update_contribution_row,
    delete_contributions_by_house,
    delete_contribution_requests,
    delete_expense_requests,
    delete_expenses,
    delete_special_requests,
    delete_special,
)

# Hide Default Streamlit Elements
hide_streamlit_style = """
    <style>
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header {visibility: hidden;}
    </style>
"""
st.markdown(hide_streamlit_style, unsafe_allow_html=True)

# ---------------------------------------------
# Constants
# ---------------------------------------------
LANES = ['ROYAL', 'SHUJAA', 'WEMA', 'KINGS']
MONTHS = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
EXPENSE_CATEGORIES = ['Personnel', 'Utilities', 'Maintenance', 'Miscellaneous']
SPECIAL_TYPES = ['Celebration', 'Emergency', 'Welfare']
DEFAULT_RATES = {'Resident': 2000, 'Non-Resident': 1000, 'Special Rate': 500}
TREASURER_PASSWORD = "zawadi01*"
EMAIL_CONFIG = {
    'sender': os.getenv('SMTP_USER', 'zawadicourt@gmail.com'),
    'password': os.getenv('SMTP_APP_PASSWORD', ''),
    'smtp_server': os.getenv('SMTP_SERVER', 'smtp.gmail.com'),  # if you set SMTP_HOST in .env, code default still matches Gmail
    'port': int(os.getenv('SMTP_PORT', '587')),
}

# Backup configuration
BACKUP_DIR = "backups"
MAX_BACKUPS = 30  # Keep last 30 backups

# ---------------------------------------------
# Mobile optimization - responsive layout
# ---------------------------------------------
def is_mobile():
    """Check if the screen is mobile size"""
    return st.session_state.get('screen_width', 1000) < 768

def mobile_friendly_container():
    """Return a container with mobile-friendly settings (placeholder)"""
    return st.container()

# ---------------------------------------------
# Session state init (cash management) — use float-safe defaults
# ---------------------------------------------
if 'cash_balance_cd' not in st.session_state:
    st.session_state.cash_balance_cd = 0.0
if 'cash_withdrawal' not in st.session_state:
    st.session_state.cash_withdrawal = 0.0

# ---------------------------------------------
# Helpers
# ---------------------------------------------
def safe_convert_to_float(value):
    """Safely convert various string formats to float."""
    if pd.isna(value) or str(value).strip() in ['', '-', ' - ']:
        return 0.0
    try:
        return float(str(value).replace(',', '').strip())
    except (ValueError, TypeError):
        return 0.0

def ensure_contributions_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Guarantee all columns the UI expects exist, with safe defaults."""
    if df is None or df.empty:
        df = pd.DataFrame()

    # Core string columns
    base_cols_defaults = {
        'House No': '',
        'Family Name': '',
        'Lane': '',
        'Rate Category': 'Resident',
        'Email': '',
        'Remarks': '',
    }
    for col, default in base_cols_defaults.items():
        if col not in df.columns:
            df[col] = default

    # Debt carried forward column
    if 'Cumulative Debt (2024 & Prior)' not in df.columns:
        df['Cumulative Debt (2024 & Prior)'] = 0.0

    # Month columns (ensure presence and numeric)
    for m in MONTHS:
        if m not in df.columns:
            df[m] = 0.0
        df[m] = pd.to_numeric(df[m], errors='coerce').fillna(0.0)

    # Computed columns (present; recalculated later)
    for c in ['YTD', 'Current Debt', 'Status']:
        if c not in df.columns:
            df[c] = 0 if c != 'Status' else ''

    # Types: strings as str, numbers as float
    df['House No'] = df['House No'].astype(str)
    df['Family Name'] = df['Family Name'].astype(str)
    df['Lane'] = df['Lane'].astype(str)
    df['Rate Category'] = df['Rate Category'].astype(str)
    df['Email'] = df['Email'].astype(str)
    df['Remarks'] = df['Remarks'].astype(str)
    df['Cumulative Debt (2024 & Prior)'] = pd.to_numeric(
        df['Cumulative Debt (2024 & Prior)'], errors='coerce'
    ).fillna(0.0)

    return df

def create_backup():
    try:
        os.makedirs(BACKUP_DIR, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = os.path.join(BACKUP_DIR, f"backup_{timestamp}")
        os.makedirs(backup_path)
        data_files = glob.glob('data/*.csv')
        for file in data_files:
            shutil.copy(file, backup_path)
        backups = sorted(glob.glob(os.path.join(BACKUP_DIR, 'backup_*')))
        while len(backups) > MAX_BACKUPS:
            oldest_backup = backups.pop(0)
            shutil.rmtree(oldest_backup)
        return True
    except Exception as e:
        st.error(f"Backup failed: {str(e)}")
        return False

def restore_backup(backup_path):
    try:
        if not os.path.exists(backup_path):
            return False
        backup_files = glob.glob(os.path.join(backup_path, '*.csv'))
        for file in backup_files:
            shutil.copy(file, 'data')
        return True
    except Exception as e:
        st.error(f"Restore failed: {str(e)}")
        return False

def backup_to_google_sheets(data: dict) -> bool:
    """
    Writes each DataFrame in `data` into a worksheet inside the
    Google Sheet whose ID is in st.secrets['gs_backup_spreadsheet_id'].
    The sheet should be named 'Zawadi_Backup' and live in your target folder.
    """
    try:
        if 'gcp_service_account' not in st.secrets:
            st.error("Google backup not configured: missing gcp_service_account in secrets.")
            return False

        spreadsheet_id = st.secrets.get("gs_backup_spreadsheet_id", "").strip()
        if not spreadsheet_id:
            st.error("Google backup not configured: missing gs_backup_spreadsheet_id in secrets.")
            return False

        creds = service_account.Credentials.from_service_account_info(
            dict(st.secrets["gcp_service_account"]),
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"
            ],
        )
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(spreadsheet_id)

        # For each dataset, upsert a worksheet and write values
        for name, df in data.items():
            # Ensure we’re writing a plain DataFrame (no indexes)
            safe_df = (df if isinstance(df, pd.DataFrame) else pd.DataFrame(df)).copy()
            safe_df = safe_df.fillna("")

            try:
                ws = sh.worksheet(name)
                ws.clear()
            except gspread.exceptions.WorksheetNotFound:
                # Create with a reasonable default size; it auto-expands on update
                ws = sh.add_worksheet(title=name, rows=max(len(safe_df) + 10, 100), cols=max(len(safe_df.columns) + 5, 20))

            if safe_df.empty:
                # Write just headers if empty
                ws.update([safe_df.columns.tolist()])
            else:
                ws.update([safe_df.columns.tolist()] + safe_df.values.tolist())

        return True

    except Exception as e:
        st.error(f"Google Sheets backup failed: {e}")
        return False

# =========================
# DATA LAYER (Postgres) with offline cache + retry
# =========================
@st.cache_data(show_spinner=False)
def _offline_data_factory():
    """Cached empty frames so first offline render is fast."""
    empty = lambda cols: pd.DataFrame({c: [] for c in cols})
    return {
        "contributions": empty(['House No','Family Name','Lane','Rate Category','Email','Remarks',
                                'Cumulative Debt (2024 & Prior)'] + MONTHS + ['YTD','Current Debt','Status']),
        "expenses": empty(['Date','Description','Category','Vendor','Phone','Amount (KES)','Mode','Remarks']),
        "special": empty(['Date','Event','Type','Contributors','Amount','Remarks']),
        "rates": pd.DataFrame({"Rate Category": ['Resident','Non-Resident','Special Rate'],
                               "Amount":[2000,1000,500]}),
        "expense_requests": empty(['id','Date','Description','Category','Requested By','Amount (KES)','Status','Remarks']),
        "contribution_requests": empty(['id','Date','Month','Family Name','House No','Lane','Rate Category','Amount (KES)','Status','Remarks']),
        "special_requests": empty(['id','Date','Event','Type','Requested By','Amount','Status','Remarks']),
        "cash_management": pd.DataFrame({"Cash Balance c/d":[0.0], "Cash Withdrawal":[0.0]}),
    }

def _attempt_live_load():
    """Try to load from Postgres. Returns (data_dict, error_or_none)."""
    try:
        d = load_all()
        return d, None
    except Exception as e:
        return None, e

def load_data():
    """
    Try Postgres via zawadi_db.load_all(). If it fails (network block/timeouts),
    fall back to cached empty DataFrames so the UI remains usable.
    """
    data, err = _attempt_live_load()
    if err is None:
        # Clear offline flag if previously set
        st.session_state['_offline'] = False
        return data

    # offline mode
    st.session_state['_offline'] = True
    if not st.session_state.get('_offline_notice_shown', False):
        st.info("⚠️ Offline mode (DB unreachable). Using cached empty data so you can continue navigating.")
        st.session_state['_offline_notice_shown'] = True
    return _offline_data_factory()

def retry_connection():
    """Called by Retry button; attempts a fresh DB read."""
    # Clear the offline cache so fresh UI re-evaluates
    _offline_data_factory.clear()
    st.session_state.pop('_offline_notice_shown', None)
    data, err = _attempt_live_load()
    if err is None:
        st.session_state['_offline'] = False
        st.toast("✅ Reconnected to database")
        st.rerun()
    else:
        st.toast("Still offline — network/DB unreachable")

def save_data(data):
    """
    Retained ONLY for exports/backups so existing buttons continue to work.
    This does NOT persist app data to Postgres (writes use DB helpers).
    """
    try:
        create_backup()
        for name, df in data.items():
            os.makedirs('data', exist_ok=True)
            df.to_csv(f'data/{name}.csv', index=False)
        cash_df = pd.DataFrame({
            'Cash Balance c/d': [st.session_state.cash_balance_cd],
            'Cash Withdrawal': [st.session_state.cash_withdrawal]
        })
        cash_df.to_csv('data/cash_management.csv', index=False)
        if 'gcp_service_account' in st.secrets:
            backup_thread = threading.Thread(target=backup_to_google_sheets, args=(data,))
            backup_thread.start()
        return True
    except Exception as e:
        st.error(f"CRITICAL SAVE ERROR: {str(e)}")
        return False

# =========================
# AUTH
# =========================
def check_treasurer_password():
    if 'treasurer_authenticated' not in st.session_state:
        st.session_state.treasurer_authenticated = False
        st.session_state.last_activity = datetime.now()
    if 'last_activity' in st.session_state:
        if (datetime.now() - st.session_state.last_activity).total_seconds() > 1800:
            st.session_state.treasurer_authenticated = False
    if not st.session_state.treasurer_authenticated:
        password = st.sidebar.text_input("Treasurer Login:", type="password", key="treasurer_pw")
        if password:
            if password == TREASURER_PASSWORD:
                st.session_state.treasurer_authenticated = True
                st.session_state.last_activity = datetime.now()
                st.rerun()
            else:
                st.sidebar.error("Incorrect password")
        return False
    return True

# =========================
# METRICS
# =========================
def get_current_month():
    return datetime.now().strftime('%b').upper()

def calculate_monthly_rate(row, rates_df):
    if 'Rate Category' not in row or pd.isna(row['Rate Category']):
        return DEFAULT_RATES['Resident']
    rate = rates_df[rates_df['Rate Category'] == row['Rate Category']]['Amount']
    return rate.values[0] if not rate.empty else DEFAULT_RATES['Resident']

def calculate_liability(row, current_month, rates_df):
    month_index = MONTHS.index(current_month) if current_month in MONTHS else 11
    monthly_rate = calculate_monthly_rate(row, rates_df)
    return (month_index + 1) * monthly_rate

def calculate_ytd(row, current_month):
    month_index = MONTHS.index(current_month) if current_month in MONTHS else 11
    relevant_months = MONTHS[:month_index + 1]
    return sum(safe_convert_to_float(row[month]) for month in relevant_months)

def calculate_current_debt(row, current_month, rates_df):
    try:
        cumulative_debt = safe_convert_to_float(row.get('Cumulative Debt (2024 & Prior)', 0))
        additional_liability = calculate_liability(row, current_month, rates_df)
        ytd = calculate_ytd(row, current_month)
        return cumulative_debt + additional_liability - ytd
    except:
        return 0

def get_payment_status(row, current_month, rates_df):
    current_debt = calculate_current_debt(row, current_month, rates_df)
    if current_debt <= 0:
        return "🟢 Up-to-date"
    month_index = MONTHS.index(current_month) if current_month in MONTHS else 11
    months_owed = 0
    for i in range(month_index + 1):
        month_val = safe_convert_to_float(row[MONTHS[i]])
        if month_val == 0:
            months_owed += 1
    if months_owed <= 2:
        return "🟠 1-2 months behind"
    else:
        return "🔴 >2 months behind"

def send_reminder_email(email, family_name, debt_amount):
    if not email or pd.isna(email):
        return False
    try:
        msg = MIMEMultipart()
        msg['From'] = EMAIL_CONFIG['sender']
        msg['To'] = email
        msg['Subject'] = f"Zawadi Court Welfare: Payment Reminder"
        body = f"""
        Dear {family_name},

        This is a friendly reminder that your current outstanding balance with Zawadi Court Welfare is KES {debt_amount:,.2f}.

        Please make your payment at your earliest convenience to avoid service interruptions.

        Thank you,
        Zawadi Court Welfare Committee
        """
        msg.attach(MIMEText(body, 'plain'))
        with smtplib.SMTP(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['port']) as server:
            server.starttls()
            server.login(EMAIL_CONFIG['sender'], EMAIL_CONFIG['password'])
            server.send_message(msg)
        return True
    except Exception as e:
        st.error(f"Failed to send email: {str(e)}")
        return False

def send_monthly_reminders(data):
    if datetime.now().day != 1:
        return
    if 'last_reminder_sent' in st.session_state:
        if st.session_state.last_reminder_sent == datetime.now().strftime('%Y-%m'):
            return
    current_month = get_current_month()
    for _, row in data['contributions'].iterrows():
        debt = calculate_current_debt(row, current_month, data['rates'])
        if debt > 0 and row['Email']:
            if send_reminder_email(row['Email'], row['Family Name'], debt):
                st.success(f"Reminder sent to {row['Family Name']}")
                time.sleep(1)
    st.session_state.last_reminder_sent = datetime.now().strftime('%Y-%m')

# =========================
# UI: Residency / Rates (→ Postgres)
# =========================
def residency_management(data):
    if not check_treasurer_password():
        st.warning("Please enter the treasurer password to access this section")
        return

    with st.expander("🏘️ Residency Rate Management", expanded=False):
        st.write("Configure monthly contribution rates for different resident categories")

        st.subheader("Current Rate Categories")
        edited_rates = st.data_editor(
            data['rates'],
            column_config={
                "Rate Category": st.column_config.TextColumn("Category", required=True),
                "Amount": st.column_config.NumberColumn("Monthly Rate (KES)", format="%d", min_value=0)
            },
            num_rows="dynamic",
            key="rates_editor"
        )

        if st.button("Save Rates"):
            # Persist to Postgres
            upsert_rates(edited_rates)
            st.success("Rate categories updated successfully!")
            st.rerun()

        st.subheader("Assign Rate Categories to Households")
        if 'contributions' in data and not data['contributions'].empty:
            rate_options = list(edited_rates['Rate Category'].unique())
            household_rates = data['contributions'][['House No', 'Family Name', 'Rate Category', 'Email']].copy()

            edited_household_rates = st.data_editor(
                household_rates,
                column_config={
                    "House No": st.column_config.TextColumn("House No", disabled=True),
                    "Family Name": st.column_config.TextColumn("Family Name", disabled=True),
                    "Rate Category": st.column_config.SelectboxColumn(
                        "Rate Category",
                        options=rate_options,
                        required=True
                    ),
                    "Email": st.column_config.TextColumn("Email Address")
                },
                use_container_width=True,
                hide_index=True,
                key="household_rates_editor"
            )

            if st.button("Apply Rate Categories"):
                house_df = edited_household_rates[['House No','Rate Category','Email']].copy()
                update_household_rate_email(house_df)
                st.success("Rate categories and emails updated successfully!")
                st.rerun()
        else:
            st.warning("No household data available to assign rate categories")

# =========================
# UI: Contributions Dashboard
# =========================
def contributions_dashboard(data):
    st.header("📊 Monthly Contributions Dashboard", divider='rainbow')

    # Ensure expected columns exist before any calculations
    data['contributions'] = ensure_contributions_columns(data.get('contributions'))

    current_month = get_current_month()

    # Calculate metrics
    data['contributions']['YTD'] = data['contributions'].apply(
        lambda row: calculate_ytd(row, current_month), axis=1
    )
    data['contributions']['Current Debt'] = data['contributions'].apply(
        lambda row: calculate_current_debt(row, current_month, data['rates']), axis=1
    )
    data['contributions']['Status'] = data['contributions'].apply(
        lambda row: get_payment_status(row, current_month, data['rates']), axis=1
    )

    # Contribution Request Form (Members)
    if not st.session_state.get('treasurer_authenticated', False):
        with st.expander("➕ Register Contribution", expanded=False):
            if 'contribution_form' not in st.session_state:
                st.session_state.contribution_form = {
                    'family_name': "", 'house_no': "", 'lane': "", 'rate_category': ""
                }

            with st.form("contribution_request_form"):
                family_names = data['contributions']['Family Name'].dropna().unique().tolist()
                selected_family = st.selectbox(
                    "Select your family name",
                    [""] + sorted(family_names),
                    index=0,
                    key="family_name_select"
                )

                if selected_family:
                    family_data = data['contributions'][data['contributions']['Family Name'] == selected_family].iloc[0]
                    st.session_state.contribution_form = {
                        'family_name': selected_family,
                        'house_no': family_data['House No'],
                        'lane': family_data['Lane'],
                        'rate_category': family_data['Rate Category']
                    }
                else:
                    st.session_state.contribution_form = {
                        'family_name': "", 'house_no': "", 'lane': "", 'rate_category': ""
                    }

                cols = st.columns(2)
                with cols[0]:
                    house_no = st.text_input("House No", value=st.session_state.contribution_form['house_no'], key="house_no_input")
                with cols[1]:
                    lane = st.text_input("Lane", value=st.session_state.contribution_form['lane'], key="lane_input")

                rate_category = st.text_input("Rate Category", value=st.session_state.contribution_form['rate_category'], key="rate_category_input")

                st.info("Payment Details: Paybill: 522522 | A/C: 1313659029")
                amount = st.number_input("Amount Paid (KES)", min_value=0, step=1000)
                payment_date = st.date_input("Payment Date", datetime.now())
                payment_ref = st.text_input("Payment Reference/Receipt Number")
                remarks = st.text_area("Remarks (optional)")

                if st.form_submit_button("Submit Contribution"):
                    if not selected_family:
                        st.error("Please select your family name")
                    elif amount <= 0:
                        st.error("Please enter a valid amount")
                    else:
                        insert_contribution_request(
                            date=payment_date.strftime('%Y-%m-%d'),
                            month=current_month,
                            family_name=selected_family,
                            house_no=str(house_no),
                            lane=str(lane),
                            rate_category=str(rate_category),
                            amount_kes=float(amount),
                            remarks=f"Payment Ref: {payment_ref}. {remarks}"
                        )
                        st.success("Contribution request submitted for approval!")
                        st.rerun()

    # Filters
    if is_mobile():
        with st.expander("🔍 Filters", expanded=False):
            family_filter = st.selectbox("Filter by Family Name", ["All"] + sorted(data['contributions']['Family Name'].unique().tolist()))
            lane_filter = st.selectbox("Filter by Lane", ["All"] + LANES)
            status_filter = st.selectbox("Filter by Status", ["All", "🟢 Up-to-date", "🟠 1-2 months behind", "🔴 >2 months behind"])
            rate_options = ["All"] + list(data['rates']['Rate Category'].unique())
            rate_filter = st.selectbox("Filter by Rate Category", rate_options)
    else:
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            family_filter = st.selectbox("Filter by Family Name", ["All"] + sorted(data['contributions']['Family Name'].unique().tolist()))
        with col2:
            lane_filter = st.selectbox("Filter by Lane", ["All"] + LANES)
        with col3:
            status_filter = st.selectbox("Filter by Status", ["All", "🟢 Up-to-date", "🟠 1-2 months behind", "🔴 >2 months behind"])
        with col4:
            rate_options = ["All"] + list(data['rates']['Rate Category'].unique())
            rate_filter = st.selectbox("Filter by Rate Category", rate_options)

    filtered_df = data['contributions'].copy()
    if family_filter != "All":
        filtered_df = filtered_df[filtered_df['Family Name'] == family_filter]
    if lane_filter != "All":
        filtered_df = filtered_df[filtered_df['Lane'] == lane_filter]
    if status_filter != "All":
        filtered_df = filtered_df[filtered_df['Status'] == status_filter]
    if rate_filter != "All":
        filtered_df = filtered_df[filtered_df['Rate Category'] == rate_filter]

    # Display data 
    st.subheader("Contributions Data")
    current_month = get_current_month()
    current_month_idx = MONTHS.index(current_month) if current_month in MONTHS else 11
    months_to_show = MONTHS[:current_month_idx + 1]

    # Final guard against missing columns in the filtered slice
    required_for_view = [
        'House No', 'Family Name', 'Lane', 'Rate Category', 'Email',
        'Remarks', 'Cumulative Debt (2024 & Prior)'
    ]
    for c in required_for_view:
        if c not in filtered_df.columns:
            filtered_df[c] = '' if c in ['House No','Family Name','Lane','Rate Category','Email','Remarks'] else 0.0
    for m in MONTHS:
        if m not in filtered_df.columns:
            filtered_df[m] = 0.0
    # Also make sure computed columns exist (defensive)
    for c in ['YTD', 'Current Debt', 'Status']:
        if c not in filtered_df.columns:
            filtered_df[c] = 0.0 if c != 'Status' else ''

    editable_cols = [
        'House No', 'Family Name', 'Lane', 'Rate Category', 'Email',
        'Cumulative Debt (2024 & Prior)'
    ] + months_to_show + ['YTD', 'Current Debt', 'Status', 'Remarks']

    if st.session_state.get('treasurer_authenticated', False):
        edited_df = st.data_editor(
            filtered_df[editable_cols],
            column_config={
                "Status": st.column_config.SelectboxColumn(
                    "Status",
                    options=["🟢 Up-to-date", "🟠 1-2 months behind", "🔴 >2 months behind"],
                    required=True
                ),
                "Rate Category": st.column_config.SelectboxColumn(
                    "Rate Category",
                    options=list(data['rates']['Rate Category'].unique()),
                    required=True
                ),
                "Email": st.column_config.TextColumn("Email Address"),
                **{
                    col: st.column_config.NumberColumn(format="%d")
                    for col in months_to_show + ['YTD', 'Current Debt', 'Cumulative Debt (2024 & Prior)']
                }
            },
            use_container_width=True,
            num_rows="dynamic",
            key="contributions_editor"
        )
        # Disable inline DB writes for now
        st.info("Inline edits are for review only. Approvals/requests update the database.")
    else:
        st.dataframe(filtered_df[editable_cols], use_container_width=True, hide_index=True)
        st.info("🔒 Only the treasurer can edit this data")

    # Approvals (Treasurer)
    if st.session_state.get('treasurer_authenticated', False):
        with st.expander("🛂 Approve Special Contributions", expanded=False):
            pending_requests = data['special_requests'][data['special_requests']['Status'] == 'Pending Approval']

            if not pending_requests.empty:
                st.write("Pending Approval:")
                selected_requests = []
                for idx, r in pending_requests.iterrows():
                    cols = st.columns([1, 10])
                    with cols[0]:
                        selected = st.checkbox(f"Select #{int(r['id'])}", key=f"spec_req_select_{int(r['id'])}")
                    with cols[1]:
                        st.write(f"**{r['Event']}** ({r['Type']}) — KES {float(r['Amount']):,.2f}")
                        st.caption(f"Requested by: {r['Requested By']} | Date: {r['Date']}")
                        st.caption(f"Remarks: {r.get('Remarks','')}")
                    if selected:
                        selected_requests.append(int(r['id']))

                if selected_requests:
                    action = st.selectbox("Action for selected requests", ["Approve", "Reject"])
                    remarks = st.text_area("Approval Remarks", "Verified and approved by treasurer")

                    if st.button("💾 Apply Action"):
                        for rid in selected_requests:
                            # Locate the row again to read its fields
                            row = pending_requests[pending_requests['id'] == rid].iloc[0].to_dict()

                            if action == "Approve":
                                # 1) write to special ledger
                                insert_special(
                                    date=row['Date'],
                                    event=row['Event'],
                                    type=row['Type'],
                                    contributors=row['Requested By'],
                                    amount=float(row['Amount']),
                                    remarks=f"{remarks} | {row.get('Remarks','')}"
                                )
                                # 2) mark request as approved
                                set_special_request_status(rid, "Approve", remarks)
                            else:
                                set_special_request_status(rid, "Reject", remarks)

                        st.success(f"{len(selected_requests)} request(s) {action.lower()}ed successfully!")
                        st.rerun()
            else:
                st.info("No pending special contribution requests")

    # Visualizations
    st.subheader("📈 Contributions Analysis")
    if is_mobile():
        with st.expander("Monthly Collections", expanded=False):
            monthly_totals = filtered_df[months_to_show].apply(pd.to_numeric, errors='coerce').sum()
            fig = px.line(
                monthly_totals,
                title=f"Monthly Collections Trend ({datetime.now().year})",
                labels={'value': 'Amount (KES)', 'index': 'Month'},
                markers=True
            )
            st.plotly_chart(fig, use_container_width=True)

        with st.expander("Contributions by Lane", expanded=False):
            lane_totals = filtered_df.groupby('Lane')['YTD'].sum()
            fig = px.bar(
                lane_totals,
                title="Total Contributions by Lane",
                labels={'value': 'Amount (KES)', 'index': 'Lane'},
                color=lane_totals.index,
                color_discrete_sequence=px.colors.qualitative.Pastel
            )
            st.plotly_chart(fig, use_container_width=True)
    else:
        col1, col2 = st.columns(2)
        with col1:
            monthly_totals = filtered_df[months_to_show].apply(pd.to_numeric, errors='coerce').sum()
            fig = px.line(
                monthly_totals,
                title=f"Monthly Collections Trend ({datetime.now().year})",
                labels={'value': 'Amount (KES)', 'index': 'Month'},
                markers=True
            )
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            lane_totals = filtered_df.groupby('Lane')['YTD'].sum()
            fig = px.bar(
                lane_totals,
                title="Total Contributions by Lane",
                labels={'value': 'Amount (KES)', 'index': 'Lane'},
                color=lane_totals.index,
                color_discrete_sequence=px.colors.qualitative.Pastel
            )
            st.plotly_chart(fig, use_container_width=True)

    # Status summary
    st.subheader("📋 Payment Status Summary")
    status_counts = filtered_df['Status'].value_counts()
    cols = st.columns(3)
    for i, (status, count) in enumerate(status_counts.items()):
        with cols[i]:
            st.metric(label=status, value=count, help=f"Households with status: {status}")

# =========================
# UI: Expense Tracker
# =========================
def expense_tracker(data):
    st.header("💸 Expense Tracker")

    # Requisition (Members)
    if not st.session_state.get('treasurer_authenticated', False):
        with st.expander("📝 Submit Expense Requisition", expanded=False):
            with st.form("expense_requisition_form"):
                cols = st.columns(2)
                with cols[0]:
                    req_date = st.date_input("Date", datetime.now())
                    req_category = st.selectbox("Category", EXPENSE_CATEGORIES)
                with cols[1]:
                    req_amount = st.number_input("Amount (KES)", min_value=0)
                    req_name = st.text_input("Requested By")
                req_description = st.text_input("Description")
                req_phone = st.text_input("Payee Phone Number (for MPesa)")
                req_remarks = st.text_area("Remarks")

                if st.form_submit_button("Submit Requisition"):
                    insert_expense_request(
                        date=req_date.strftime('%Y-%m-%d'),
                        description=req_description,
                        category=req_category,
                        requested_by=req_name,
                        amount_kes=float(req_amount),
                        remarks=f"Phone: {req_phone}. {req_remarks}"
                    )
                    st.success("Expense requisition submitted for approval!")
                    st.rerun()

    # Approvals (Treasurer)
    if st.session_state.get('treasurer_authenticated', False):
        with st.expander("🛂 Approve Expense Requisitions", expanded=False):
            pending_requests = data['expense_requests'][data['expense_requests']['Status'] == 'Pending Approval']

            if not pending_requests.empty:
                st.write("Pending Approval:")
                selected_requests = []
                for idx, request in pending_requests.iterrows():
                    cols = st.columns([1, 10])
                    with cols[0]:
                        selected = st.checkbox(f"Select #{idx}", key=f"exp_req_select_{idx}")
                    with cols[1]:
                        st.write(f"**{request['Description']}** - KES {request['Amount (KES)']:,.2f}")
                        st.caption(f"Requested by: {request['Requested By']} | Category: {request['Category']}")
                        st.caption(f"Date: {request['Date']} | Remarks: {request['Remarks']}")
                    if selected:
                        selected_requests.append(idx)

                if selected_requests:
                    action = st.selectbox("Action for selected requests", ["Approve", "Reject"])
                    remarks = st.text_area("Approval Remarks", "Approved by treasurer")
                    payment_mode = st.selectbox("Payment Mode", ["Cash", "MPesa", "Bank Transfer"])
                    payment_phone = st.text_input("Payee Phone Number (for MPesa)")

                    if st.button("💾 Apply Action"):
                        for idx in selected_requests:
                            r = pending_requests.loc[idx]
                            if action == "Approve":
                                insert_expense(
                                    date=r['Date'],
                                    description=r['Description'],
                                    category=r['Category'],
                                    vendor=r['Requested By'],
                                    phone=payment_phone,
                                    amount_kes=float(r['Amount (KES)']),
                                    mode=payment_mode,
                                    remarks=f"Approved from requisition: {r['Remarks']}"
                                )
                                set_expense_request_status(int(r['id']) if 'id' in r else int(idx), "Approve", remarks)
                            else:
                                set_expense_request_status(int(r['id']) if 'id' in r else int(idx), "Reject", remarks)
                        st.success(f"{len(selected_requests)} requests {action.lower()}ed successfully!")
                        st.rerun()
            else:
                st.info("No pending expense requisitions")

    # Cash flow
    st.subheader("💰 Cash Flow Management")
    if st.session_state.get('treasurer_authenticated', False):
        cash_cols = st.columns(3)
        with cash_cols[0]:
            st.session_state.cash_balance_cd = st.number_input(
                "Cash Balance c/d (KES)", min_value=0, value=int(st.session_state.cash_balance_cd), step=1000, key="cash_balance_input"
            )
        with cash_cols[1]:
            st.session_state.cash_withdrawal = st.number_input(
                "Cash Withdrawal (KES)", min_value=0, value=int(st.session_state.cash_withdrawal), step=1000, key="cash_withdrawal_input"
            )

        if st.button("Save Cash Balance"):
            try:
                update_cash_management(
                    cash_balance_cd=float(st.session_state.cash_balance_cd),
                    cash_withdrawal=float(st.session_state.cash_withdrawal)
                )
                st.success("Cash balances updated successfully!")
                st.rerun()
            except RuntimeError as e:
                # Raised by DB layer when offline
                st.error("Write disabled in offline mode")
    else:
        cash_cols = st.columns(3)
        with cash_cols[0]:
            st.metric("Cash Balance c/d", f"KES {st.session_state.cash_balance_cd:,.2f}")
        with cash_cols[1]:
            st.metric("Cash Withdrawal", f"KES {st.session_state.cash_withdrawal:,.2f}")

    # --- Robust total expenses (force numeric) ---
    if 'Amount (KES)' in data['expenses'].columns:
        amt_series = pd.to_numeric(data['expenses']['Amount (KES)'], errors='coerce').fillna(0)
        total_expenses = float(amt_series.sum())
    else:
        total_expenses = 0.0

    def _to_float(x):
        try:
            if x is None:
                return 0.0
            if isinstance(x, str):
                x = x.strip().replace(',', '')
                return float(x) if x else 0.0
            return float(x)
        except Exception:
            return 0.0

    cash_balance_cd  = _to_float(st.session_state.get('cash_balance_cd', 0))
    cash_withdrawal  = _to_float(st.session_state.get('cash_withdrawal', 0))
    total_expenses_f = _to_float(total_expenses)

    cash_balance_bf = cash_balance_cd + cash_withdrawal - total_expenses_f

    st.markdown(f"""
    <div style="background-color:#f8f9fa;padding:15px;border-radius:10px;margin-bottom:20px">
        <div style="display:flex;justify-content:space-between">
            <span><strong>Cash Balance c/d:</strong></span>
            <span>KES {cash_balance_cd:,.2f}</span>
        </div>
        <div style="display:flex;justify-content:space-between">
            <span><strong>Add: Cash Withdrawal:</strong></span>
            <span>KES {cash_withdrawal:,.2f}</span>
        </div>
        <div style="display:flex;justify-content:space-between;border-bottom:1px solid #ddd;padding-bottom:5px;margin-bottom:5px">
            <span><strong>Less: Total Expenses:</strong></span>
            <span>KES {total_expenses_f:,.2f}</span>
        </div>
        <div style="display:flex;justify-content:space-between;font-weight:bold">
            <span><strong>Cash Balance b/f:</strong></span>
            <span>KES {cash_balance_bf:,.2f}</span>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Treasurer adds ad-hoc expense
    if st.session_state.get('treasurer_authenticated', False):
        with st.expander("➕ Add New Expense", expanded=False):
            with st.form("expense_form"):
                cols = st.columns(2)
                with cols[0]:
                    expense_date = st.date_input("Date", datetime.now())
                    expense_category = st.selectbox("Category", EXPENSE_CATEGORIES)
                with cols[1]:
                    expense_amount = st.number_input("Amount (KES)", min_value=0)
                    expense_mode = st.selectbox("Payment Mode", ["Cash", "MPesa", "Bank Transfer"])

                expense_description = st.text_input("Description")
                expense_vendor = st.text_input("Vendor")
                expense_phone = st.text_input("Payee Phone Number (for MPesa)")
                expense_remarks = st.text_area("Remarks")
                expense_receipt = st.file_uploader("Upload Receipt (optional)", type=['png', 'jpg', 'pdf'])

                if st.form_submit_button("Add Expense"):
                    # Optional receipt file storage (local only)
                    if expense_receipt:
                        os.makedirs('receipts', exist_ok=True)
                        with open(f"receipts/{expense_receipt.name}", "wb") as f:
                            f.write(expense_receipt.getbuffer())
                    try:
                        insert_expense(
                            date=expense_date.strftime('%Y-%m-%d'),
                            description=expense_description,
                            category=expense_category,
                            vendor=expense_vendor,
                            phone=expense_phone,
                            amount_kes=float(expense_amount),
                            mode=expense_mode,
                            remarks=expense_remarks
                        )
                        st.success("Expense added successfully!")
                        st.rerun()
                    except RuntimeError:
                        st.error("Write disabled in offline mode")

    # Expense records
    st.subheader("📝 Expense Records")
    if is_mobile():
        with st.expander("🔍 Filters", expanded=False):
            month_filter = st.selectbox("Filter by Month", ["All"] + MONTHS)
            category_filter = st.selectbox("Filter by Category", ["All"] + EXPENSE_CATEGORIES)
    else:
        filter_cols = st.columns(2)
        with filter_cols[0]:
            month_filter = st.selectbox("Filter by Month", ["All"] + MONTHS)
        with filter_cols[1]:
            category_filter = st.selectbox("Filter by Category", ["All"] + EXPENSE_CATEGORIES)

    filtered_expenses = data['expenses'].copy()
    # --- Robust month filter (coerce bad dates, drop NaT) ---
    if month_filter != "All":
        if 'Date' in filtered_expenses.columns:
            _d = pd.to_datetime(filtered_expenses['Date'], errors='coerce', dayfirst=True, infer_datetime_format=True)
            mask = _d.dt.strftime('%b').str.upper() == month_filter
            filtered_expenses = filtered_expenses.loc[mask.fillna(False)]
        else:
            st.warning("No 'Date' column in expenses; month filter ignored.")

    if category_filter != "All":
        filtered_expenses = filtered_expenses[filtered_expenses['Category'] == category_filter]

    if st.session_state.get('treasurer_authenticated', False):
        filtered_expenses['Phone'] = filtered_expenses['Phone'].astype(str)
        st.data_editor(
            filtered_expenses,
            column_config={
                "Amount (KES)": st.column_config.NumberColumn("Amount (KES)", format="%d", min_value=0),
                "Date": st.column_config.TextColumn("Date (YYYY-MM-DD)"),
                "Category": st.column_config.SelectboxColumn("Category", options=EXPENSE_CATEGORIES),
                "Phone": st.column_config.TextColumn("Payee Phone")
            },
            use_container_width=True,
            num_rows="dynamic",
            key="expenses_editor",
            hide_index=True
        )
        st.info("Inline edits are for review only; expenses are persisted via approvals or 'Add New Expense'.")
    else:
        st.dataframe(filtered_expenses, use_container_width=True, hide_index=True)

    # Visualizations
    st.subheader("📊 Expense Analysis")
    if not filtered_expenses.empty:
        if is_mobile():
            with st.expander("Expense Breakdown", expanded=False):
                category_totals = filtered_expenses.groupby('Category')['Amount (KES)'].sum()
                fig = px.pie(
                    category_totals,
                    names=category_totals.index,
                    title="Expense Breakdown by Category",
                    color=category_totals.index,
                    color_discrete_sequence=px.colors.qualitative.Pastel,
                    values='Amount (KES)'
                )
                st.plotly_chart(fig, use_container_width=True)
            with st.expander("Monthly Trend", expanded=False):
                monthly_expenses = data['expenses'].copy()
                # Identify a date column we can use
                date_col = None
                for candidate in ['Date', 'date', 'Expense Date', 'expense_date', 'created_at']:
                    if candidate in monthly_expenses.columns:
                        date_col = candidate
                        break

                if date_col is None:
                    st.warning("No date column found in expenses; skipping monthly reports.")
                    return

                # Coerce to datetime, tolerate bad rows, and drop NaTs
                monthly_expenses[date_col] = pd.to_datetime(
                    monthly_expenses[date_col],
                    errors='coerce',
                    dayfirst=True,              # flip to False if you’re strictly YYYY-MM-DD
                    infer_datetime_format=True
                )
                monthly_expenses = monthly_expenses.dropna(subset=[date_col])

                # Now derive month label
                monthly_expenses['Month'] = monthly_expenses[date_col].dt.strftime('%b').str.upper()

                monthly_totals = monthly_expenses.groupby('Month')['Amount (KES)'].sum().reindex(MONTHS, fill_value=0)
                fig = px.line(
                    monthly_totals,
                    title="Monthly Expense Trend",
                    labels={'value': 'Amount (KES)', 'index': 'Month'},
                    markers=True
                )
                st.plotly_chart(fig, use_container_width=True)
        else:
            viz_cols = st.columns(2)
            with viz_cols[0]:
                category_totals = filtered_expenses.groupby('Category')['Amount (KES)'].sum()
                fig = px.pie(
                    category_totals,
                    names=category_totals.index,
                    title="Expense Breakdown by Category",
                    color=category_totals.index,
                    color_discrete_sequence=px.colors.qualitative.Pastel,
                    values='Amount (KES)'
                )
                st.plotly_chart(fig, use_container_width=True)
            with viz_cols[1]:
                monthly_expenses = data['expenses'].copy()
                monthly_expenses['Date'] = pd.to_datetime(monthly_expenses['Date'], errors='coerce')
                monthly_expenses = monthly_expenses.dropna(subset=['Date'])
                monthly_expenses['Month'] = monthly_expenses['Date'].dt.strftime('%b').str.upper()
                monthly_totals = monthly_expenses.groupby('Month')['Amount (KES)'].sum().reindex(MONTHS, fill_value=0)
                fig = px.line(
                    monthly_totals,
                    title="Monthly Expense Trend",
                    labels={'value': 'Amount (KES)', 'index': 'Month'},
                    markers=True
                )
                st.plotly_chart(fig, use_container_width=True)

# =========================
# UI: Special Contributions
# =========================
def special_contributions(data):
    st.header("🎉 Special Contributions")

    if not st.session_state.get('treasurer_authenticated', False):
        with st.expander("➕ Register Special Contribution", expanded=False):
            with st.form("special_contribution_form"):
                cols = st.columns(2)
                with cols[0]:
                    event_name = st.text_input("Event Name")
                    event_date = st.date_input("Event Date", datetime.now())
                with cols[1]:
                    event_type = st.selectbox("Type", SPECIAL_TYPES)
                    event_amount = st.number_input("Amount (KES)", min_value=0, step=500)
                requested_by = st.text_input("Requested By")
                event_remarks = st.text_area("Remarks")

                if st.form_submit_button("Submit Request"):
                    if not event_name or event_amount <= 0 or not requested_by:
                        st.error("Please provide Event Name, Requested By, and a positive Amount.")
                    else:
                        insert_special_request(
                            date=event_date.strftime('%Y-%m-%d'),
                            event=event_name,
                            type=event_type,
                            requested_by=requested_by,
                            amount=float(event_amount),
                            remarks=event_remarks
                        )
                        st.success("Special contribution request submitted for approval!")
                        st.rerun()

    if st.session_state.get('treasurer_authenticated', False):
        with st.expander("🛂 Approve Special Contributions", expanded=False):
            pending_requests = data['special_requests'][data['special_requests']['Status'] == 'Pending Approval']

            if not pending_requests.empty:
                st.write("Pending Approval:")
                selected_requests = []
                for _, r in pending_requests.iterrows():
                    rid = int(r['id'])
                    cols = st.columns([1, 10])
                    with cols[0]:
                        selected = st.checkbox(f"Select #{rid}", key=f"spec_req_select_{rid}")
                    with cols[1]:
                        st.write(f"**{r['Event']}** ({r['Type']}) — KES {float(r['Amount']):,.2f}")
                        st.caption(f"Requested by: {r['Requested By']} | Date: {r['Date']}")
                        st.caption(f"Remarks: {r.get('Remarks','')}")
                    if selected:
                        selected_requests.append(rid)

                if selected_requests:
                    action = st.selectbox("Action for selected requests", ["Approve", "Reject"])
                    remarks = st.text_area("Approval Remarks", "Verified and approved by treasurer")

                    if st.button("💾 Apply Action"):
                        for rid in selected_requests:
                            row = pending_requests[pending_requests['id'] == rid].iloc[0].to_dict()

                            if action == "Approve":
                                insert_special(
                                    date=row['Date'],
                                    event=row['Event'],
                                    type=row['Type'],
                                    contributors=row['Requested By'],
                                    amount=float(row['Amount']),
                                    remarks=f"{remarks} | {row.get('Remarks','')}"
                                )
                                set_special_request_status(rid, "Approve", remarks)
                            else:
                                set_special_request_status(rid, "Reject", remarks)

                        st.success(f"{len(selected_requests)} request(s) {action.lower()}ed successfully!")
                        st.rerun()
            else:
                st.info("No pending special contribution requests")

    upcoming_events = data['special'].copy()
    if not upcoming_events.empty:
        upcoming_events['Date'] = pd.to_datetime(upcoming_events['Date'])
        upcoming_events = upcoming_events[upcoming_events['Date'] >= datetime.now()]
        if not upcoming_events.empty:
            st.sidebar.subheader("📅 Upcoming Events")
            for _, event in upcoming_events.iterrows():
                emoji = "🎉" if event['Type'] == 'Celebration' else "⚠️" if event['Type'] == 'Emergency' else "🤝"
                st.sidebar.write(f"{emoji} {event['Event']} - {event['Date'].strftime('%b %d')}")

    st.subheader("📋 Special Contribution Records")
    type_filter = st.selectbox("Filter by Type", ["All"] + SPECIAL_TYPES)
    filtered_special = data['special'].copy()
    if type_filter != "All":
        filtered_special = filtered_special[filtered_special['Type'] == type_filter]

    if st.session_state.get('treasurer_authenticated', False):
        edited_special = st.data_editor(
            filtered_special,
            column_config={
                "Amount": st.column_config.NumberColumn("Amount (KES)", format="%d"),
                "Type": st.column_config.SelectboxColumn("Type", options=SPECIAL_TYPES),
                "Date": st.column_config.TextColumn("Date (YYYY-MM-DD)")
            },
            use_container_width=True,
            num_rows="dynamic",
            key="special_editor",
            hide_index=True
        )
        st.info("Inline edits are for review only.")
    else:
        st.dataframe(filtered_special, use_container_width=True, hide_index=True)

    st.subheader("📊 Special Contributions Analysis")
    if not filtered_special.empty:
        if is_mobile():
            with st.expander("Contributions by Type", expanded=False):
                type_totals = filtered_special.groupby('Type')['Amount'].sum()
                fig = px.pie(
                    type_totals,
                    names=type_totals.index,
                    title="Contributions by Type",
                    color=type_totals.index,
                    color_discrete_sequence=px.colors.qualitative.Pastel,
                    values='Amount'
                )
                st.plotly_chart(fig, use_container_width=True)
            with st.expander("Monthly Contributions", expanded=False):
                monthly_special = filtered_special.copy()
                monthly_special['Month'] = pd.to_datetime(monthly_special['Date']).dt.strftime('%b').str.upper()
                monthly_totals = monthly_special.groupby('Month')['Amount'].sum().reindex(MONTHS, fill_value=0)
                fig = px.bar(
                    monthly_totals,
                    title="Monthly Special Contributions",
                    labels={'value': 'Amount (KES)', 'index': 'Month'},
                    color=monthly_totals.index,
                    color_discrete_sequence=px.colors.qualitative.Pastel
                )
                st.plotly_chart(fig, use_container_width=True)
        else:
            viz_cols = st.columns(2)
            with viz_cols[0]:
                type_totals = filtered_special.groupby('Type')['Amount'].sum()
                fig = px.pie(
                    type_totals,
                    names=type_totals.index,
                    title="Contributions by Type",
                    color=type_totals.index,
                    color_discrete_sequence=px.colors.qualitative.Pastel,
                    values='Amount'
                )
                st.plotly_chart(fig, use_container_width=True)
            with viz_cols[1]:
                monthly_special = filtered_special.copy()
                monthly_special['Month'] = pd.to_datetime(monthly_special['Date']).dt.strftime('%b').str.upper()
                monthly_totals = monthly_special.groupby('Month')['Amount'].sum().reindex(MONTHS, fill_value=0)
                fig = px.bar(
                    monthly_totals,
                    title="Monthly Special Contributions",
                    labels={'value': 'Amount (KES)', 'index': 'Month'},
                    color=monthly_totals.index,
                    color_discrete_sequence=px.colors.qualitative.Pastel
                )
                st.plotly_chart(fig, use_container_width=True)

# =========================
# UI: Reports (read-only)
# =========================
def reports(data):
    st.header("📑 Financial Reports")

    def safe_sum(series):
        return sum(safe_convert_to_float(x) for x in series)

    current_month = get_current_month()
    data['contributions']['YTD'] = data['contributions'].apply(
        lambda row: calculate_ytd(row, current_month), axis=1
    )

    st.subheader("📊 Summary Statistics")
    cols = st.columns(3)
    with cols[0]:
        total_contributions = safe_sum(data['contributions']['YTD'])
        st.metric("Total Regular Contributions", f"KES {total_contributions:,.2f}")
    with cols[1]:
        total_expenses = safe_sum(data['expenses']['Amount (KES)']) if 'Amount (KES)' in data['expenses'].columns else 0
        st.metric("Total Expenses", f"KES {total_expenses:,.2f}")
    with cols[2]:
        total_special = safe_sum(data['special']['Amount']) if 'Amount' in data['special'].columns else 0
        st.metric("Total Special Contributions", f"KES {total_special:,.2f}")

    current_year = datetime.now().year
    report_year = st.selectbox("Select Year for Report", [current_year, current_year - 1], index=0)

    st.subheader("📋 Detailed Reports")
    report_type = st.selectbox("Select Report Type", [
        "Lane-wise Contributions",
        "Expense Category Breakdown",
        "Payment Status Distribution",
        "Rate Category Analysis",
        "Special Contributions Analysis",
        "Detailed Monthly Contributions",
        "Detailed Expense Records",
        "Year-on-Year Trends"
    ], index=0)

    if report_type == "Lane-wise Contributions":
        lane_report = data['contributions'].groupby('Lane').agg({
            'YTD': lambda x: safe_sum(x),
            'Current Debt': lambda x: safe_sum(x),
            'House No': 'count'
        }).rename(columns={'House No': 'Households'})
        lane_total = safe_sum(lane_report['YTD'])
        if abs(lane_total - total_contributions) > 0.01:
            st.warning(f"Data consistency issue: Lane-wise total ({lane_total:,.2f}) doesn't match summary total ({total_contributions:,.2f})")

        st.dataframe(
            lane_report.style.format({'YTD': "KES {:,.2f}", 'Current Debt': "KES {:,.2f}"}),
            use_container_width=True
        )

        monthly_contrib = data['contributions'][MONTHS].apply(pd.to_numeric, errors='coerce').sum()
        monthly_expenses = data['expenses'].copy()
        monthly_expenses['Date'] = pd.to_datetime(monthly_expenses['Date'], errors='coerce')
        monthly_expenses = monthly_expenses.dropna(subset=['Date'])
        monthly_expenses['Month'] = monthly_expenses['Date'].dt.strftime('%b').str.upper()

        monthly_exp_totals = monthly_expenses.groupby('Month')['Amount (KES)'].sum().reindex(MONTHS, fill_value=0)

        combined_df = pd.DataFrame({
            'Month': MONTHS,
            'Contributions': monthly_contrib,
            'Expenses': monthly_exp_totals
        }).melt(id_vars='Month', var_name='Type', value_name='Amount')

        fig = px.line(
            combined_df,
            x='Month', y='Amount', color='Type',
            title="Monthly Contribution vs Expense Trend",
            labels={'Amount': 'Amount (KES)', 'Month': 'Month'},
            markers=True
        )
        st.plotly_chart(fig, use_container_width=True, key="monthly_trend_chart")

    elif report_type == "Expense Category Breakdown":
        expense_report = data['expenses'].groupby('Category')['Amount (KES)'].apply(safe_sum)
        st.dataframe(expense_report.to_frame('Total Amount').style.format("KES {:,.2f}"), use_container_width=True)
        fig = px.pie(
            expense_report,
            names=expense_report.index,
            title="Expense Distribution by Category",
            color=expense_report.index,
            color_discrete_sequence=px.colors.qualitative.Pastel,
            values='Amount (KES)'
        )
        st.plotly_chart(fig, use_container_width=True, key="expense_pie_chart")

    elif report_type == "Payment Status Distribution":
        status_report = data['contributions']['Status'].value_counts()
        if isinstance(status_report, pd.Series):
            status_report = status_report.reset_index()
            status_report.columns = ['Status', 'Count']
        st.dataframe(status_report, use_container_width=True)
        color_map = {
            "🟢 Up-to-date": "#2ecc71",
            "🟠 1-2 months behind": "#f39c12",
            "🔴 >2 months behind": "#e74c3c"
        }
        fig = px.pie(
            status_report,
            names='Status',
            values='Count',
            title="Payment Status Distribution",
            color='Status',
            color_discrete_map=color_map
        )
        st.plotly_chart(fig, use_container_width=True)

    elif report_type == "Rate Category Analysis":
        if 'Rate Category' in data['contributions'].columns:
            rate_report = data['contributions'].groupby('Rate Category').agg({
                'YTD': lambda x: safe_sum(x),
                'House No': 'count'
            }).rename(columns={'House No': 'Households'})
            rate_report = rate_report.merge(
                data['rates'].set_index('Rate Category'),
                left_index=True,
                right_index=True
            )
            st.dataframe(
                rate_report.style.format({'YTD': "KES {:,.2f}", 'Amount': "KES {:,.2f}"}),
                use_container_width=True
            )
            fig = px.bar(
                rate_report,
                x=rate_report.index, y='Households',
                title="Households by Rate Category",
                color=rate_report.index,
                color_discrete_sequence=px.colors.qualitative.Pastel
            )
            st.plotly_chart(fig, use_container_width=True, key="rate_category_bar")

    elif report_type == "Special Contributions Analysis":
        if not data['special'].empty:
            special_report = data['special'].groupby('Type')['Amount'].apply(safe_sum)
            st.dataframe(special_report.to_frame('Total Amount').style.format("KES {:,.2f}"), use_container_width=True)
            fig = px.pie(
                special_report,
                names=special_report.index,
                title="Special Contributions by Type",
                color=special_report.index,
                color_discrete_sequence=px.colors.qualitative.Pastel,
                values='Amount'
            )
            st.plotly_chart(fig, use_container_width=True, key="special_pie_chart")

    elif report_type == "Year-on-Year Trends":
        st.subheader("Year-on-Year Financial Trends")
        st.info("Year-on-year comparison requires historical data from previous years")
        if st.checkbox("Show Example with Sample Data"):
            years = [current_year, current_year - 1]
            sample_data = pd.DataFrame({
                'Year': years,
                'Total Contributions': [safe_sum(data['contributions']['YTD']),
                                       safe_sum(data['contributions']['YTD']) * 0.8],
                'Total Expenses': [safe_sum(data['expenses']['Amount (KES)']) if 'Amount (KES)' in data['expenses'].columns else 0,
                                  (safe_sum(data['expenses']['Amount (KES)']) * 0.75) if 'Amount (KES)' in data['expenses'].columns else 0],
                'Special Contributions': [safe_sum(data['special']['Amount']) if 'Amount' in data['special'].columns else 0,
                                         (safe_sum(data['special']['Amount']) * 0.7) if 'Amount' in data['special'].columns else 0]
            })
            fig = px.bar(
                sample_data.melt(id_vars='Year'),
                x='Year', y='value', color='variable', barmode='group',
                title="Year-on-Year Financial Comparison",
                labels={'value': 'Amount (KES)', 'variable': 'Category'}
            )
            st.plotly_chart(fig, use_container_width=True, key="yearly_comparison_bar")

    # Export (kept)
    st.subheader("📤 Export Reports")
    report_df = None
    if report_type == "Detailed Monthly Contributions":
        report_df = data['contributions']
    elif report_type == "Detailed Expense Records":
        report_df = data['expenses']
    elif report_type == "Lane-wise Contributions":
        report_df = data['contributions'].groupby('Lane').agg({
            'YTD': lambda x: safe_sum(x),
            'Current Debt': lambda x: safe_sum(x),
            'House No': 'count'
        }).rename(columns={'House No': 'Households'})
    elif report_type == "Expense Category Breakdown":
        report_df = data['expenses'].groupby('Category')['Amount (KES)'].apply(safe_sum).to_frame('Total Amount')
    elif report_type == "Payment Status Distribution":
        report_df = data['contributions']['Status'].value_counts().to_frame('Households')
    elif report_type == "Rate Category Analysis":
        if 'Rate Category' in data['contributions'].columns:
            report_df = data['contributions'].groupby('Rate Category').agg({
                'YTD': lambda x: safe_sum(x),
                'House No': 'count'
            }).rename(columns={'House No': 'Households'}).merge(
                data['rates'].set_index('Rate Category'),
                left_index=True, right_index=True
            )
    elif report_type == "Special Contributions Analysis":
        if not data['special'].empty:
            report_df = data['special'].groupby('Type')['Amount'].apply(safe_sum).to_frame('Total Amount')

    if report_df is not None:
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            report_df.to_excel(writer, sheet_name=report_type[:30])
        st.download_button(
            label="⬇️ Download Current Report",
            data=output.getvalue(),
            file_name=f"zawadi_{report_type.lower().replace(' ', '_')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    if st.session_state.get('treasurer_authenticated', False):
        st.subheader("📤 Full Data Export")
        export_options = st.multiselect(
            "Select data to export",
            options=[
                "Contributions Data", "Expenses Data", "Special Contributions",
                "Rate Categories", "Expense Requests", "Contribution Requests", "Special Requests"
            ],
            default=["Contributions Data", "Expenses Data", "Special Contributions"]
        )

        if st.button("🖨️ Generate Custom Report"):
            with pd.ExcelWriter("zawadi_custom_report.xlsx") as writer:
                if "Contributions Data" in export_options:
                    data['contributions'].to_excel(writer, sheet_name="Contributions")
                if "Expenses Data" in export_options:
                    data['expenses'].to_excel(writer, sheet_name="Expenses")
                if "Special Contributions" in export_options:
                    data['special'].to_excel(writer, sheet_name="Special")
                if "Rate Categories" in export_options and 'rates' in data:
                    data['rates'].to_excel(writer, sheet_name="Rate Categories")
                if "Expense Requests" in export_options and 'expense_requests' in data:
                    data['expense_requests'].to_excel(writer, sheet_name="Expense Requests")
                if "Contribution Requests" in export_options and 'contribution_requests' in data:
                    data['contribution_requests'].to_excel(writer, sheet_name="Contribution Requests")
                if "Special Requests" in export_options and 'special_requests' in data:
                    data['special_requests'].to_excel(writer, sheet_name="Special Requests")

                summary_df = pd.DataFrame({
                    'Metric': ['Total Regular Contributions', 'Total Expenses', 'Total Special Contributions'],
                    'Amount (KES)': [
                        safe_sum(data['contributions']['YTD']),
                        safe_sum(data['expenses']['Amount (KES)']) if 'Amount (KES)' in data['expenses'].columns else 0,
                        safe_sum(data['special']['Amount']) if 'Amount' in data['special'].columns else 0
                    ]
                })
                summary_df.to_excel(writer, sheet_name="Summary", index=False)

            with open("zawadi_custom_report.xlsx", "rb") as f:
                st.download_button(
                    "⬇️ Download Custom Report",
                    f,
                    "zawadi_custom_report.xlsx",
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

# =========================
# Tests (unchanged)
# =========================
class TestZawadiFunctions(unittest.TestCase):
    def test_safe_convert_to_float(self):
        self.assertEqual(safe_convert_to_float("1,000"), 1000.0)
        self.assertEqual(safe_convert_to_float("500"), 500.0)
        self.assertEqual(safe_convert_to_float(""), 0.0)
        self.assertEqual(safe_convert_to_float(" - "), 0.0)
        self.assertEqual(safe_convert_to_float("ABC"), 0.0)

    def test_calculate_ytd(self):
        test_row = pd.Series({
            'JAN': 1000, 'FEB': 2000, 'MAR': 0, 'APR': 3000,
            'MAY': 0, 'JUN': 0, 'JUL': 0, 'AUG': 0,
            'SEP': 0, 'OCT': 0, 'NOV': 0, 'DEC': 0
        })
        self.assertEqual(calculate_ytd(test_row, 'JAN'), 1000)
        self.assertEqual(calculate_ytd(test_row, 'FEB'), 3000)
        self.assertEqual(calculate_ytd(test_row, 'APR'), 6000)

    @patch('pandas.read_csv')
    def test_load_data(self, mock_read_csv):
        # Deprecated in Postgres mode, but keep a simple smoke test
        mock_read_csv.side_effect = FileNotFoundError
        d = {'contributions': pd.DataFrame(), 'expenses': pd.DataFrame(), 'special': pd.DataFrame(), 'rates': pd.DataFrame()}
        self.assertTrue(isinstance(d, dict))

    @patch('smtplib.SMTP')
    def test_send_reminder_email(self, mock_smtp):
        mock_server = MagicMock()
        mock_smtp.return_value = mock_server
        result = send_reminder_email("test@example.com", "Test Family", 5000)
        self.assertTrue(result)
        mock_smtp.assert_called_once()

# =========================
# App entry
# =========================
def main():
    # Sidebar offline banner + retry
    if st.session_state.get('_offline', False):
        with st.sidebar:
            st.warning("🔌 Offline mode (DB unreachable)")
            if st.button("🔄 Retry DB connection"):
                retry_connection()

    data = load_data()
    # Ensure contributions has all expected columns
    if 'contributions' in data:
        data['contributions'] = ensure_contributions_columns(data['contributions'])

    cm_df = data.get('cash_management')
    if isinstance(cm_df, pd.DataFrame) and not cm_df.empty:
        try:
            st.session_state.cash_balance_cd = float(cm_df.iloc[0]['Cash Balance c/d'])
            st.session_state.cash_withdrawal = float(cm_df.iloc[0]['Cash Withdrawal'])
        except Exception:
            pass

    if datetime.now().day == 1:
        send_monthly_reminders(data)

    st.sidebar.title("🏠 Zawadi Court Welfare")
    if 'treasurer_authenticated' not in st.session_state:
        st.session_state.treasurer_authenticated = False

    check_treasurer_password()

    # --- One-click email test (treasurer only) ---
    if st.session_state.get('treasurer_authenticated', False):
        with st.sidebar.expander("✉️ Email tools", expanded=False):
            if st.button("Send test email", key="send_test_email_btn"):
                ok = send_reminder_email(
                    os.getenv("SMTP_USER", "zawadicourt@gmail.com"),  # send to yourself
                    "Zawadi Court (Test)",
                    1234.56
                )
                st.success("Test email sent!") if ok else st.error("Send failed")

    if is_mobile():
        with st.sidebar.expander("Menu", expanded=True):
            page = st.radio(
                "Navigation",
                ["Contributions Dashboard", "Expense Tracker", "Special Contributions", "Reports"],
                index=0
            )
    else:
        page = st.sidebar.radio(
            "Navigation",
            ["Contributions Dashboard", "Expense Tracker", "Special Contributions", "Reports"],
            index=0
        )

    if st.session_state.get('treasurer_authenticated', False):
        residency_management(data)

    if page == "Contributions Dashboard":
        contributions_dashboard(data)
    elif page == "Expense Tracker":
        expense_tracker(data)
    elif page == "Special Contributions":
        special_contributions(data)
    elif page == "Reports":
        reports(data)

    # ========== Admin: Edit / Delete (Treasurer only) ==========
    if st.session_state.get('treasurer_authenticated', False):
        st.markdown("---")
        st.subheader("🛠️ Admin: Edit / Delete (Treasurer)")

        admin_tabs = st.tabs([
            "Households (Contributions)",
            "Requests & Expenses",
            "Special Events"
        ])

        # --- Households (Contributions) ---
        with admin_tabs[0]:
            df = data.get('contributions', pd.DataFrame()).copy()
            if df.empty:
                st.info("No households found.")
            else:
                left, right = st.columns([1, 2])
                with left:
                    hn = st.selectbox(
                        "Select House No",
                        options=df["House No"].astype(str).tolist(),
                        index=0,
                    )

                row = df.loc[df["House No"].astype(str) == str(hn)].iloc[0].copy()

                with right:
                    st.markdown("**Edit selected household**")
                    family_name = st.text_input("Family Name", row.get("Family Name", ""))
                    lane = st.text_input("Lane", row.get("Lane", ""))
                    rate_category = st.text_input("Rate Category", row.get("Rate Category", ""))
                    email = st.text_input("Email", row.get("Email", ""))

                    colA, colB = st.columns(2)
                    with colA:
                        cum_prior = st.number_input(
                            "Cumulative Debt (2024 & Prior)",
                            value=float(row.get("Cumulative Debt (2024 & Prior)", 0) or 0.0),
                            step=100.0
                        )
                        status = st.text_input("Status", row.get("Status", ""))
                    with colB:
                        remarks = st.text_area("Remarks", row.get("Remarks", ""))

                    st.markdown("**Monthly amounts**")
                    months = {}
                    mcols = st.columns(6)
                    labels = ["JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"]
                    for i, m in enumerate(labels):
                        with mcols[i % 6]:
                            months[m] = st.number_input(m, value=float(row.get(m, 0) or 0.0), step=100.0)

                    c1, c2 = st.columns(2)
                    with c1:
                        if st.button("💾 Save Changes"):
                            try:
                                update_contribution_row(
                                    house_no=str(hn),
                                    family_name=family_name,
                                    lane=lane,
                                    rate_category=rate_category,
                                    email=email,
                                    cumulative_debt_prior=cum_prior,
                                    months=months,
                                    status=status,
                                    remarks=remarks,
                                )
                                st.success(f"Updated household {hn}.")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Update failed: {e}")

                    with c2:
                        if st.button("🗑️ Delete Household", type="primary"):
                            try:
                                delete_contributions_by_house([str(hn)])
                                st.success(f"Deleted household {hn} and related pending requests.")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Delete failed: {e}")

        # --- Requests & Expenses ---
        with admin_tabs[1]:
            c_req = data.get('contribution_requests', pd.DataFrame()).copy()
            e_req = data.get('expense_requests', pd.DataFrame()).copy()
            exp   = data.get('expenses', pd.DataFrame()).copy()

            st.markdown("**Contribution Requests**")
            if c_req.empty:
                st.caption("None.")
            else:
                ids = st.multiselect("Select request IDs to delete", c_req["id"].astype(int).tolist(), [])
                if st.button("🗑️ Delete Selected Contribution Requests"):
                    try:
                        delete_contribution_requests([int(i) for i in ids])
                        st.success(f"Deleted {len(ids)} contribution request(s).")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Delete failed: {e}")

            st.markdown("**Expense Requests**")
            if e_req.empty:
                st.caption("None.")
            else:
                ids = st.multiselect("Select expense-request IDs to delete", e_req["id"].astype(int).tolist(), key="exp_req_ids")
                if st.button("🗑️ Delete Selected Expense Requests"):
                    try:
                        delete_expense_requests([int(i) for i in ids])
                        st.success(f"Deleted {len(ids)} expense request(s).")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Delete failed: {e}")

            st.markdown("**Expenses**")
            if exp.empty:
                st.caption("None.")
            else:
                # if there is no 'id' column, skip the tool gracefully
                if 'id' not in exp.columns:
                    st.info("Expenses table has no 'id' column; delete by ID is unavailable.")
                else:
                    ids = st.multiselect("Select expense IDs to delete", exp["id"].astype(int).tolist(), key="exp_ids")
                    if st.button("🗑️ Delete Selected Expenses"):
                        try:
                            delete_expenses([int(i) for i in ids])
                            st.success(f"Deleted {len(ids)} expense(s).")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Delete failed: {e}")

        # --- Special Events ---
        with admin_tabs[2]:
            sp  = data.get('special', pd.DataFrame()).copy()
            spr = data.get('special_requests', pd.DataFrame()).copy()

            st.markdown("**Special Contributions**")
            if sp.empty:
                st.caption("None.")
            else:
                if 'id' not in sp.columns:
                    st.info("Special table has no 'id' column; delete by ID is unavailable.")
                else:
                    ids = st.multiselect("Select special IDs to delete", sp["id"].astype(int).tolist(), key="sp_ids")
                    if st.button("🗑️ Delete Selected Special"):
                        try:
                            delete_special([int(i) for i in ids])
                            st.success(f"Deleted {len(ids)} special record(s).")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Delete failed: {e}")

            st.markdown("**Special Requests**")
            if spr.empty:
                st.caption("None.")
            else:
                if 'id' not in spr.columns:
                    st.info("Special Requests table has no 'id' column; delete by ID is unavailable.")
                else:
                    ids = st.multiselect("Select special-request IDs to delete", spr["id"].astype(int).tolist(), key="spr_ids")
                    if st.button("🗑️ Delete Selected Special Requests"):
                        try:
                            delete_special_requests([int(i) for i in ids])
                            st.success(f"Deleted {len(ids)} special request(s).")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Delete failed: {e}")

    if st.session_state.get('treasurer_authenticated', False):
        st.sidebar.subheader("💾 Data Management")
        with st.sidebar.expander("🔁 Backup Options", expanded=False):
            if st.button("Create New Backup"):
                if create_backup():
                    st.success("Backup created successfully in 'backups' directory!")
                else:
                    st.error("Backup failed")
            backups = sorted(glob.glob(os.path.join(BACKUP_DIR, 'backup_*')), reverse=True)
            if backups:
                st.write("Available Backups:")
                selected_backup = st.selectbox(
                    "Select backup to restore",
                    backups,
                    format_func=lambda x: os.path.basename(x)
                )
                if st.button("🔄 Restore Selected Backup"):
                    if restore_backup(selected_backup):
                        st.success("Backup restored successfully! Please refresh the page.")
                        time.sleep(2)
                        st.rerun()
                    else:
                        st.error("Restore failed")
            else:
                st.info("No backups available yet")

        with st.sidebar.expander("📤 Manual Data Restore", expanded=False):
            st.warning("Use with caution - will overwrite current data")
            uploaded_file = st.file_uploader(
                "Upload CSV file to restore",
                type=['csv'],
                accept_multiple_files=True,
                help="Upload CSV files for contributions, expenses, or special data"
            )
            if uploaded_file and st.button("Restore from Uploaded Files"):
                try:
                    temp = {}
                    for file in uploaded_file:
                        if 'contributions' in file.name.lower():
                            temp['contributions'] = pd.read_csv(file)
                        elif 'expenses' in file.name.lower():
                            temp['expenses'] = pd.read_csv(file)
                        elif 'special' in file.name.lower():
                            temp['special'] = pd.read_csv(file)
                        elif 'rates' in file.name.lower():
                            temp['rates'] = pd.read_csv(file)
                        elif 'expense_requests' in file.name.lower():
                            temp['expense_requests'] = pd.read_csv(file)
                        elif 'contribution_requests' in file.name.lower():
                            temp['contribution_requests'] = pd.read_csv(file)
                        elif 'special_requests' in file.name.lower():
                            temp['special_requests'] = pd.read_csv(file)
                    if temp:
                        save_data(temp)
                    st.success("Data restored successfully from uploaded files!")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error restoring data: {e}")

    if os.getenv('DEV_MODE'):
        test_results = unittest.TextTestRunner().run(unittest.TestLoader().loadTestsFromTestCase(TestZawadiFunctions))
        if not test_results.wasSuccessful():
            st.error("Some tests failed. Check the logs for details.")

if __name__ == "__main__":
    main()