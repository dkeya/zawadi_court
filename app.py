import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from datetime import datetime, date
import os
import hashlib
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import time
import unittest
from unittest.mock import patch, MagicMock
import io

# Page configuration with mobile-friendly settings
st.set_page_config(
    page_title="Zawadi Court Welfare System",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': 'https://example.com/help',
        'Report a bug': "https://example.com/bug",
        'About': "# Zawadi Court Welfare System"
    }
)

# Constants
LANES = ['ROYAL', 'SHUJAA', 'WEMA', 'KINGS']
MONTHS = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
EXPENSE_CATEGORIES = ['Personnel', 'Utilities', 'Maintenance', 'Miscellaneous']
SPECIAL_TYPES = ['Celebration', 'Emergency', 'Welfare']
DEFAULT_RATES = {'Resident': 2000, 'Non-Resident': 1000, 'Special Rate': 500}
TREASURER_PASSWORD = "zawadi01*"
EMAIL_CONFIG = {
    'sender': 'welfare@zawadicourt.com',
    'password': 'emailpassword',
    'smtp_server': 'smtp.example.com',
    'port': 587
}

# Mobile optimization - responsive layout functions
def is_mobile():
    """Check if the screen is mobile size"""
    return st.session_state.get('screen_width', 1000) < 768

def mobile_friendly_container():
    """Return a container with mobile-friendly settings"""
    if is_mobile():
        return st.container()
    return st.container()

# Initialize session state for cash management
if 'cash_balance_cd' not in st.session_state:
    st.session_state.cash_balance_cd = 0
if 'cash_withdrawal' not in st.session_state:
    st.session_state.cash_withdrawal = 0

def safe_convert_to_float(value):
    """Safely convert various string formats to float"""
    if pd.isna(value) or str(value).strip() in ['', '-', ' - ']:
        return 0.0
    try:
        return float(str(value).replace(',', '').strip())
    except (ValueError, TypeError):
        return 0.0

def load_data():
    """Load and clean data from CSV files"""
    data_files = {
        'contributions': 'data/contributions.csv',
        'expenses': 'data/expenses.csv',
        'special': 'data/special.csv',
        'rates': 'data/rates.csv',
        'expense_requests': 'data/expense_requests.csv',
        'contribution_requests': 'data/contribution_requests.csv',
        'special_requests': 'data/special_requests.csv',
        'cash_management': 'data/cash_management.csv'
    }
    
    os.makedirs('data', exist_ok=True)
    
    data = {}
    for name, file in data_files.items():
        try:
            if name == 'contributions':
                df = pd.read_csv(file, dtype=str)
                numeric_cols = MONTHS + ['Cumulative Debt (2024 & Prior)', 'YTD', 'Current Debt']
                for col in numeric_cols:
                    if col in df.columns:
                        df[col] = df[col].apply(safe_convert_to_float)
                df['House No'] = df['House No'].astype(str)
                df['Family Name'] = df['Family Name'].astype(str)
                df['Lane'] = df['Lane'].astype(str)
                if 'Remarks' in df.columns:
                    df['Remarks'] = df['Remarks'].astype(str)
                if 'Rate Category' not in df.columns:
                    df['Rate Category'] = 'Resident'
                if 'Email' not in df.columns:
                    df['Email'] = ''
                data[name] = df
            elif name == 'rates':
                try:
                    rates_df = pd.read_csv(file)
                    if rates_df.empty:
                        rates_df = pd.DataFrame({
                            'Rate Category': list(DEFAULT_RATES.keys()),
                            'Amount': list(DEFAULT_RATES.values())
                        })
                    data[name] = rates_df
                except:
                    data[name] = pd.DataFrame({
                        'Rate Category': list(DEFAULT_RATES.keys()),
                        'Amount': list(DEFAULT_RATES.values())
                    })
            elif name == 'expenses':
                df = pd.read_csv(file)
                if 'Date' in df.columns:
                    # Handle different date formats
                    try:
                        df['Date'] = pd.to_datetime(df['Date'], dayfirst=True).dt.strftime('%Y-%m-%d')
                    except:
                        try:
                            df['Date'] = pd.to_datetime(df['Date'], format='%d/%m/%Y %H:%M').dt.strftime('%Y-%m-%d')
                        except:
                            df['Date'] = pd.to_datetime(df['Date'], errors='coerce').dt.strftime('%Y-%m-%d')
                if 'Amount (KES)' in df.columns:
                    df['Amount (KES)'] = df['Amount (KES)'].apply(safe_convert_to_float)
                if 'Phone' not in df.columns:
                    df['Phone'] = ''
                data[name] = df
            elif name == 'special':
                df = pd.read_csv(file)
                if 'Date' in df.columns:
                    df['Date'] = pd.to_datetime(df['Date'], dayfirst=True).dt.strftime('%Y-%m-%d')
                if 'Amount' in df.columns:
                    df['Amount'] = df['Amount'].apply(safe_convert_to_float)
                data[name] = df
            elif name in ['expense_requests', 'contribution_requests', 'special_requests']:
                df = pd.read_csv(file)
                if 'Date' in df.columns:
                    df['Date'] = pd.to_datetime(df['Date'], dayfirst=True).dt.strftime('%Y-%m-%d')
                if 'Amount (KES)' in df.columns:
                    df['Amount (KES)'] = df['Amount (KES)'].apply(safe_convert_to_float)
                if 'Amount' in df.columns:
                    df['Amount'] = df['Amount'].apply(safe_convert_to_float)
                data[name] = df
            elif name == 'cash_management':
                try:
                    df = pd.read_csv(file)
                    if not df.empty:
                        st.session_state.cash_balance_cd = df['Cash Balance c/d'].iloc[0]
                        st.session_state.cash_withdrawal = df['Cash Withdrawal'].iloc[0]
                except FileNotFoundError:
                    pass
                    
        except FileNotFoundError:
            if name == 'contributions':
                columns = ['House No', 'Family Name', 'Lane', 'Rate Category', 'Email',
                          'Cumulative Debt (2024 & Prior)'] + MONTHS + ['YTD', 'Current Debt', 'Remarks']
                data[name] = pd.DataFrame(columns=columns)
            elif name == 'expenses':
                columns = ['Date', 'Description', 'Category', 'Vendor', 'Phone', 'Amount (KES)', 'Mode', 'Remarks', 'Receipt']
                data[name] = pd.DataFrame(columns=columns)
            elif name == 'special':
                columns = ['Event', 'Date', 'Type', 'Contributors', 'Amount', 'Remarks']
                data[name] = pd.DataFrame(columns=columns)
            elif name == 'rates':
                data[name] = pd.DataFrame({
                    'Rate Category': list(DEFAULT_RATES.keys()),
                    'Amount': list(DEFAULT_RATES.values())
                })
            elif name == 'expense_requests':
                columns = ['Date', 'Description', 'Category', 'Requested By', 'Amount (KES)', 'Status', 'Remarks']
                data[name] = pd.DataFrame(columns=columns)
            elif name == 'contribution_requests':
                columns = ['Date', 'Month', 'Family Name', 'House No', 'Lane', 'Rate Category', 'Amount (KES)', 'Status', 'Remarks']
                data[name] = pd.DataFrame(columns=columns)
            elif name == 'special_requests':
                columns = ['Date', 'Event', 'Type', 'Requested By', 'Amount', 'Status', 'Remarks']
                data[name] = pd.DataFrame(columns=columns)
    
    return data

def save_data(data):
    """Save data to CSV files"""
    for name, df in data.items():
        df.to_csv(f'data/{name}.csv', index=False)
    
    # Save cash management data
    cash_df = pd.DataFrame({
        'Cash Balance c/d': [st.session_state.cash_balance_cd],
        'Cash Withdrawal': [st.session_state.cash_withdrawal]
    })
    cash_df.to_csv('data/cash_management.csv', index=False)

def check_treasurer_password():
    """Check if user has entered the treasurer password"""
    if 'treasurer_authenticated' not in st.session_state:
        st.session_state.treasurer_authenticated = False
        st.session_state.last_activity = datetime.now()
    
    # Add session timeout (30 minutes)
    if 'last_activity' in st.session_state:
        if (datetime.now() - st.session_state.last_activity).total_seconds() > 1800:
            st.session_state.treasurer_authenticated = False
    
    if not st.session_state.treasurer_authenticated:
        password = st.sidebar.text_input("Enter Treasurer Password:", type="password", key="treasurer_pw")
        if password:
            if password == TREASURER_PASSWORD:
                st.session_state.treasurer_authenticated = True
                st.session_state.last_activity = datetime.now()
                st.rerun()
            else:
                st.sidebar.error("Incorrect password")
        return False
    return True

def get_current_month():
    """Get current month as 3-letter abbreviation"""
    return datetime.now().strftime('%b').upper()

def calculate_monthly_rate(row, rates_df):
    """Get the monthly rate for a household"""
    if 'Rate Category' not in row or pd.isna(row['Rate Category']):
        return DEFAULT_RATES['Resident']
    rate = rates_df[rates_df['Rate Category'] == row['Rate Category']]['Amount']
    return rate.values[0] if not rate.empty else DEFAULT_RATES['Resident']

def calculate_liability(row, current_month, rates_df):
    """Calculate additional liability based on rate category and months elapsed"""
    month_index = MONTHS.index(current_month) if current_month in MONTHS else 11
    monthly_rate = calculate_monthly_rate(row, rates_df)
    return (month_index + 1) * monthly_rate  # +1 because months are 0-indexed

def calculate_ytd(row, current_month):
    """Calculate Year-to-Date contributions"""
    month_index = MONTHS.index(current_month) if current_month in MONTHS else 11
    relevant_months = MONTHS[:month_index + 1]
    return sum(safe_convert_to_float(row[month]) for month in relevant_months)

def calculate_current_debt(row, current_month, rates_df):
    """Calculate current debt with new formula"""
    try:
        cumulative_debt = safe_convert_to_float(row.get('Cumulative Debt (2024 & Prior)', 0))
        additional_liability = calculate_liability(row, current_month, rates_df)
        ytd = calculate_ytd(row, current_month)
        return cumulative_debt + additional_liability - ytd
    except:
        return 0

def get_payment_status(row, current_month, rates_df):
    """Determine payment status for a household"""
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
    """Send email reminder about outstanding debt"""
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
    """Send monthly reminders to all members with outstanding balances"""
    if datetime.now().day != 1:  # Only run on the 1st of the month
        return
    
    if 'last_reminder_sent' in st.session_state:
        if st.session_state.last_reminder_sent == datetime.now().strftime('%Y-%m'):
            return  # Already sent this month
    
    current_month = get_current_month()
    for _, row in data['contributions'].iterrows():
        debt = calculate_current_debt(row, current_month, data['rates'])
        if debt > 0 and row['Email']:
            if send_reminder_email(row['Email'], row['Family Name'], debt):
                st.success(f"Reminder sent to {row['Family Name']}")
                time.sleep(1)  # Rate limiting
    
    st.session_state.last_reminder_sent = datetime.now().strftime('%Y-%m')

def residency_management(data):
    """Manage residency rates and categories"""
    if not check_treasurer_password():
        st.warning("Please enter the treasurer password to access this section")
        return
        
    with st.expander("🏘️ Residency Rate Management", expanded=False):
        st.write("Configure monthly contribution rates for different resident categories")
        
        # Display current rates
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
            data['rates'] = edited_rates
            save_data(data)
            st.success("Rate categories updated successfully!")
        
        # Apply rate categories to households
        st.subheader("Assign Rate Categories to Households")
        if 'contributions' in data and not data['contributions'].empty:
            rate_options = list(data['rates']['Rate Category'].unique())
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
                data['contributions']['Rate Category'] = data['contributions']['House No'].map(
                    edited_household_rates.set_index('House No')['Rate Category']
                )
                data['contributions']['Email'] = data['contributions']['House No'].map(
                    edited_household_rates.set_index('House No')['Email']
                )
                save_data(data)
                st.success("Rate categories and emails updated successfully!")
        else:
            st.warning("No household data available to assign rate categories")

def contributions_dashboard(data):
    st.header("📊 Monthly Contributions Dashboard", divider='rainbow')
    
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
    
    # Contribution Request Form - Visible to all members
    if not st.session_state.get('treasurer_authenticated', False):
        with st.expander("➕ Register Contribution", expanded=False):
            with st.form("contribution_request_form"):
                # Get unique family names
                family_names = data['contributions']['Family Name'].unique().tolist()
                family_names = [name for name in family_names if str(name) != 'nan']
            
                # Family name selection with auto-population
                family_name = st.selectbox(
                    "Select your family name",
                    [""] + sorted(family_names),
                    index=0,
                    key="family_name_select"
                )
            
                # Auto-populate details immediately when family name is selected
                if family_name:
                    family_data = data['contributions'][data['contributions']['Family Name'] == family_name].iloc[0]
                    house_no = family_data['House No']
                    lane = family_data['Lane']
                    rate_category = family_data['Rate Category']
                else:
                    house_no = ""
                    lane = ""
                    rate_category = ""
            
                # Display fields (now editable)
                cols = st.columns(2)
                with cols[0]:
                    house_no = st.text_input("House No", value=house_no, key="house_no_input")
                with cols[1]:
                    lane = st.text_input("Lane", value=lane, key="lane_input")
            
                rate_category = st.text_input("Rate Category", value=rate_category, key="rate_category_input")
            
                # Contribution details
                st.info("Payment Details: Paybill: 522522 | A/C: 1313659029")
                amount = st.number_input("Amount Paid (KES)", min_value=0, step=1000)
                payment_date = st.date_input("Payment Date", datetime.now())
                payment_ref = st.text_input("Payment Reference/Receipt Number")
                remarks = st.text_area("Remarks (optional)")
            
                if st.form_submit_button("Submit Contribution"):
                    if not family_name:
                        st.error("Please select your family name")
                    elif amount <= 0:
                        st.error("Please enter a valid amount")
                    else:
                        new_request = {
                            'Date': payment_date.strftime('%Y-%m-%d'),
                            'Month': current_month,
                            'Family Name': family_name,
                            'House No': house_no,
                            'Lane': lane,
                            'Rate Category': rate_category,
                            'Amount (KES)': amount,
                            'Status': 'Pending Approval',
                            'Remarks': f"Payment Ref: {payment_ref}. {remarks}"
                        }
                        data['contribution_requests'] = pd.concat(
                            [data['contribution_requests'], pd.DataFrame([new_request])], 
                            ignore_index=True
                        )
                        save_data(data)
                        st.success("Contribution request submitted for approval!")
                        st.rerun()

    # Filters - responsive layout
    if is_mobile():
        with st.expander("🔍 Filters", expanded=False):
            family_filter = st.selectbox("Filter by Family Name", 
                                        ["All"] + sorted(data['contributions']['Family Name'].unique().tolist()))
            lane_filter = st.selectbox("Filter by Lane", ["All"] + LANES)
            status_filter = st.selectbox("Filter by Status", 
                                       ["All", "🟢 Up-to-date", "🟠 1-2 months behind", "🔴 >2 months behind"])
            rate_options = ["All"] + list(data['rates']['Rate Category'].unique())
            rate_filter = st.selectbox("Filter by Rate Category", rate_options)
    else:
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            family_filter = st.selectbox("Filter by Family Name", 
                                        ["All"] + sorted(data['contributions']['Family Name'].unique().tolist()))
        with col2:
            lane_filter = st.selectbox("Filter by Lane", ["All"] + LANES)
        with col3:
            status_filter = st.selectbox("Filter by Status", 
                                       ["All", "🟢 Up-to-date", "🟠 1-2 months behind", "🔴 >2 months behind"])
        with col4:
            rate_options = ["All"] + list(data['rates']['Rate Category'].unique())
            rate_filter = st.selectbox("Filter by Rate Category", rate_options)
    
    # Apply filters
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
    current_month_idx = MONTHS.index(current_month) if current_month in MONTHS else 11
    months_to_show = MONTHS[:current_month_idx + 1]
    
    # Reorder columns to put Remarks last
    editable_cols = ['House No', 'Family Name', 'Lane', 'Rate Category', 'Email',
                    'Cumulative Debt (2024 & Prior)'] + months_to_show + ['YTD', 'Current Debt', 'Status', 'Remarks']
    
    # Only allow editing if treasurer is authenticated
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
                **{col: st.column_config.NumberColumn(format="%d") for col in months_to_show + ['YTD', 'Current Debt', 'Cumulative Debt (2024 & Prior)']}
            },
            use_container_width=True,
            num_rows="dynamic",
            key="contributions_editor"
        )
        
        if st.button("💾 Save Changes"):
            for col in editable_cols:
                data['contributions'].loc[edited_df.index, col] = edited_df[col]
            # Recalculate YTD after saving changes
            data['contributions']['YTD'] = data['contributions'].apply(
                lambda row: calculate_ytd(row, current_month), axis=1
            )
            save_data(data)
            st.success("Changes saved successfully!")
    else:
        st.dataframe(
            filtered_df[editable_cols],
            use_container_width=True,
            hide_index=True
        )
        st.info("🔒 Only the treasurer can edit this data")
    
    # Approve Contribution Requests - Only for treasurer
    if st.session_state.get('treasurer_authenticated', False):
        with st.expander("🛂 Approve Contribution Requests", expanded=False):
            pending_requests = data['contribution_requests'][data['contribution_requests']['Status'] == 'Pending Approval']
        
            if not pending_requests.empty:
                st.write("Pending Approval:")
            
                # Add checkboxes for each request
                selected_requests = []
                for idx, request in pending_requests.iterrows():
                    cols = st.columns([1, 10])
                    with cols[0]:
                        selected = st.checkbox(f"Select #{idx}", key=f"cont_req_select_{idx}")
                    with cols[1]:
                        st.write(f"**{request['Family Name']}** (House {request['House No']}, {request['Lane']})")
                        st.write(f"Month: {request['Month']} | Amount: KES {request['Amount (KES)']:,.2f}")
                        st.caption(f"Payment Date: {request['Date']} | Remarks: {request['Remarks']}")
                
                    if selected:
                        selected_requests.append(idx)
            
                if selected_requests:
                    action = st.selectbox("Action for selected requests", ["Approve", "Reject"])
                    remarks = st.text_area("Approval Remarks", "Verified and approved by treasurer")
                
                    if st.button("💾 Apply Action"):
                        for idx in selected_requests:
                            data['contribution_requests'].at[idx, 'Status'] = action
                            data['contribution_requests'].at[idx, 'Remarks'] = f"{remarks} - {data['contribution_requests'].at[idx, 'Remarks']}"
                        
                            # If approved, add to contributions
                            if action == "Approve":
                                family_name = data['contribution_requests'].at[idx, 'Family Name']
                                month = data['contribution_requests'].at[idx, 'Month']
                                amount = data['contribution_requests'].at[idx, 'Amount (KES)']
                            
                                # Find the family in contributions
                                family_idx = data['contributions'][data['contributions']['Family Name'] == family_name].index
                                if not family_idx.empty:
                                    data['contributions'].at[family_idx[0], month] = amount
                                    # Recalculate YTD after updating contribution
                                    data['contributions'].at[family_idx[0], 'YTD'] = calculate_ytd(
                                        data['contributions'].iloc[family_idx[0]], current_month
                                    )
                    
                        save_data(data)
                        st.success(f"{len(selected_requests)} contribution requests {action.lower()}ed successfully!")
                        st.rerun()
            else:
                st.info("No pending contribution requests")
    
    # Visualizations - responsive layout
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
    status_colors = {
        "🟢 Up-to-date": "#2ecc71",
        "🟠 1-2 months behind": "#f39c12",
        "🔴 >2 months behind": "#e74c3c"
    }
    
    for i, (status, count) in enumerate(status_counts.items()):
        with cols[i]:
            st.metric(
                label=status,
                value=count,
                help=f"Households with status: {status}"
            )

def expense_tracker(data):
    st.header("💸 Expense Tracker")
    
    # Expense Requisition Section - Visible to all members
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
                    new_request = {
                        'Date': req_date.strftime('%Y-%m-%d'),
                        'Category': req_category,
                        'Description': req_description,
                        'Requested By': req_name,
                        'Amount (KES)': req_amount,
                        'Status': 'Pending Approval',
                        'Remarks': f"Phone: {req_phone}. {req_remarks}"
                    }
                    data['expense_requests'] = pd.concat(
                        [data['expense_requests'], pd.DataFrame([new_request])],
                        ignore_index=True
                    )
                    save_data(data)
                    st.success("Expense requisition submitted for approval!")
                    st.rerun()
  
    # Approve Expense Requisitions - Only for treasurer
    if st.session_state.get('treasurer_authenticated', False):
        with st.expander("🛂 Approve Expense Requisitions", expanded=False):
            pending_requests = data['expense_requests'][data['expense_requests']['Status'] == 'Pending Approval']
        
            if not pending_requests.empty:
                st.write("Pending Approval:")
            
                # Add checkboxes for each request
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
                            data['expense_requests'].at[idx, 'Status'] = action
                            data['expense_requests'].at[idx, 'Remarks'] = f"{remarks} - {data['expense_requests'].at[idx, 'Remarks']}"
                        
                            # If approved, add to expenses
                            if action == "Approve":
                                new_expense = {
                                    'Date': data['expense_requests'].at[idx, 'Date'],
                                    'Description': data['expense_requests'].at[idx, 'Description'],
                                    'Category': data['expense_requests'].at[idx, 'Category'],
                                    'Vendor': data['expense_requests'].at[idx, 'Requested By'],
                                    'Phone': payment_phone,
                                    'Amount (KES)': data['expense_requests'].at[idx, 'Amount (KES)'],
                                    'Mode': payment_mode,
                                    'Remarks': f"Approved from requisition: {data['expense_requests'].at[idx, 'Remarks']}",
                                    'Receipt': None
                                }
                                data['expenses'] = pd.concat([data['expenses'], pd.DataFrame([new_expense])], ignore_index=True)
                    
                        save_data(data)
                        st.success(f"{len(selected_requests)} requests {action.lower()}ed successfully!")
                        st.rerun()
            else:
                st.info("No pending expense requisitions")
    
    # Cash balance section
    st.subheader("💰 Cash Flow Management")
    
    if st.session_state.get('treasurer_authenticated', False):
        # Treasurer can edit cash balances
        cash_cols = st.columns(3)
        with cash_cols[0]:
            st.session_state.cash_balance_cd = st.number_input(
                "Cash Balance c/d (KES)", 
                min_value=0, 
                value=int(st.session_state.cash_balance_cd), 
                step=1000,
                key="cash_balance_input"
            )
        with cash_cols[1]:
            st.session_state.cash_withdrawal = st.number_input(
                "Cash Withdrawal (KES)", 
                min_value=0, 
                value=int(st.session_state.cash_withdrawal), 
                step=1000,
                key="cash_withdrawal_input"
            )
        
        if st.button("Save Cash Balance"):
            save_data(data)
            st.success("Cash balances updated successfully!")
    else:
        # Members can only view cash balances
        cash_cols = st.columns(3)
        with cash_cols[0]:
            st.metric("Cash Balance c/d", f"KES {st.session_state.cash_balance_cd:,.2f}")
        with cash_cols[1]:
            st.metric("Cash Withdrawal", f"KES {st.session_state.cash_withdrawal:,.2f}")
    
    # Calculate and display cash summary
    total_expenses = data['expenses']['Amount (KES)'].sum() if 'Amount (KES)' in data['expenses'].columns else 0
    cash_balance_bf = st.session_state.cash_balance_cd + st.session_state.cash_withdrawal - total_expenses
    
    st.markdown(f"""
    <div style="background-color:#f8f9fa;padding:15px;border-radius:10px;margin-bottom:20px">
        <div style="display:flex;justify-content:space-between">
            <span><strong>Cash Balance c/d:</strong></span>
            <span>KES {st.session_state.cash_balance_cd:,.2f}</span>
        </div>
        <div style="display:flex;justify-content:space-between">
            <span><strong>Add: Cash Withdrawal:</strong></span>
            <span>KES {st.session_state.cash_withdrawal:,.2f}</span>
        </div>
        <div style="display:flex;justify-content:space-between;border-bottom:1px solid #ddd;padding-bottom:5px;margin-bottom:5px">
            <span><strong>Less: Total Expenses:</strong></span>
            <span>KES {total_expenses:,.2f}</span>
        </div>
        <div style="display:flex;justify-content:space-between;font-weight:bold">
            <span><strong>Cash Balance b/f:</strong></span>
            <span>KES {cash_balance_bf:,.2f}</span>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Add new expense form - Only for treasurer
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
                    new_expense = {
                        'Date': expense_date.strftime('%Y-%m-%d'),
                        'Category': expense_category,
                        'Description': expense_description,
                        'Vendor': expense_vendor,
                        'Phone': expense_phone,
                        'Amount (KES)': expense_amount,
                        'Mode': expense_mode,
                        'Remarks': expense_remarks,
                        'Receipt': expense_receipt.name if expense_receipt else None
                    }
                    
                    if expense_receipt:
                        os.makedirs('receipts', exist_ok=True)
                        with open(f"receipts/{expense_receipt.name}", "wb") as f:
                            f.write(expense_receipt.getbuffer())
                    
                    data['expenses'] = pd.concat([data['expenses'], pd.DataFrame([new_expense])], ignore_index=True)
                    save_data(data)
                    st.success("Expense added successfully!")
                    st.rerun()
    
    # Expense records
    st.subheader("📝 Expense Records")
    
    # Filters
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
    
    # Apply filters
    filtered_expenses = data['expenses'].copy()
    if month_filter != "All":
        filtered_expenses = filtered_expenses[
            pd.to_datetime(filtered_expenses['Date']).dt.strftime('%b').str.upper() == month_filter
        ]
    if category_filter != "All":
        filtered_expenses = filtered_expenses[filtered_expenses['Category'] == category_filter]
    
    # Display editable table - Only for treasurer
    if st.session_state.get('treasurer_authenticated', False):
        filtered_expenses['Phone'] = filtered_expenses['Phone'].astype(str)
        edited_expenses = st.data_editor(
            filtered_expenses,
            column_config={
                "Amount (KES)": st.column_config.NumberColumn(
                    "Amount (KES)", 
                    format="%d",
                    min_value=0
                ),
                "Date": st.column_config.TextColumn(
                    "Date (YYYY-MM-DD)"
                ),
                "Category": st.column_config.SelectboxColumn(
                    "Category",
                    options=EXPENSE_CATEGORIES
                ),
                "Phone": st.column_config.TextColumn(
                    "Payee Phone"
                )
            },
            use_container_width=True,
            num_rows="dynamic",
            key="expenses_editor",
            hide_index=True
        )
        
        if st.button("💾 Save Expense Changes"):
            # Convert date strings back to datetime format before saving
            edited_expenses['Date'] = pd.to_datetime(edited_expenses['Date'])
            data['expenses'].loc[edited_expenses.index] = edited_expenses
            save_data(data)
            st.success("Expense changes saved successfully!")
    else:
        st.dataframe(
            filtered_expenses,
            use_container_width=True,
            hide_index=True
        )
    
    # Visualizations - responsive layout
    st.subheader("📊 Expense Analysis")
    
    if is_mobile():
        with st.expander("Expense Breakdown", expanded=False):
            if not filtered_expenses.empty:
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
            if not data['expenses'].empty:
                monthly_expenses = data['expenses'].copy()
                monthly_expenses['Month'] = pd.to_datetime(monthly_expenses['Date']).dt.strftime('%b').str.upper()
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
            if not filtered_expenses.empty:
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
            if not data['expenses'].empty:
                monthly_expenses = data['expenses'].copy()
                monthly_expenses['Month'] = pd.to_datetime(monthly_expenses['Date']).dt.strftime('%b').str.upper()
                monthly_totals = monthly_expenses.groupby('Month')['Amount (KES)'].sum().reindex(MONTHS, fill_value=0)
                fig = px.line(
                    monthly_totals, 
                    title="Monthly Expense Trend",
                    labels={'value': 'Amount (KES)', 'index': 'Month'},
                    markers=True
                )
                st.plotly_chart(fig, use_container_width=True)

def special_contributions(data):
    st.header("🎉 Special Contributions")
    
    # Special Contribution Request Form - Visible to all members
    if not st.session_state.get('treasurer_authenticated', False):
        with st.expander("➕ Register Special Contribution", expanded=False):
            with st.form("special_contribution_form"):
                cols = st.columns(2)
                with cols[0]:
                    event_name = st.text_input("Event Name")
                    event_date = st.date_input("Event Date", datetime.now())
                with cols[1]:
                    event_type = st.selectbox("Type", SPECIAL_TYPES)
                    event_amount = st.number_input("Amount (KES)", min_value=0)
            
                requested_by = st.text_input("Requested By")
                event_remarks = st.text_area("Remarks")
            
                if st.form_submit_button("Submit Request"):
                    new_request = {
                        'Date': event_date.strftime('%Y-%m-%d'),
                        'Event': event_name,
                        'Type': event_type,
                        'Requested By': requested_by,
                        'Amount': event_amount,
                        'Status': 'Pending Approval',
                        'Remarks': event_remarks
                    }
                    data['special_requests'] = pd.concat(
                        [data['special_requests'], pd.DataFrame([new_request])],
                        ignore_index=True
                    )
                    save_data(data)
                    st.success("Special contribution request submitted for approval!")
                    st.rerun()

    # Approve Special Contributions - Only for treasurer
    if st.session_state.get('treasurer_authenticated', False):
        with st.expander("🛂 Approve Special Contributions", expanded=False):
            pending_requests = data['special_requests'][data['special_requests']['Status'] == 'Pending Approval']
        
            if not pending_requests.empty:
                st.write("Pending Approval:")
            
                # Add checkboxes for each request
                selected_requests = []
                for idx, request in pending_requests.iterrows():
                    cols = st.columns([1, 10])
                    with cols[0]:
                        selected = st.checkbox(f"Select #{idx}", key=f"spec_req_select_{idx}")
                    with cols[1]:
                        st.write(f"**{request['Event']}** ({request['Type']})")
                        st.write(f"Amount: KES {request['Amount']:,.2f} | Requested by: {request['Requested By']}")
                        st.caption(f"Date: {request['Date']} | Remarks: {request['Remarks']}")
                
                    if selected:
                        selected_requests.append(idx)
            
                if selected_requests:
                    action = st.selectbox("Action for selected requests", ["Approve", "Reject"])
                    remarks = st.text_area("Approval Remarks", "Verified and approved by treasurer")
                
                    if st.button("💾 Apply Action"):
                        for idx in selected_requests:
                            data['special_requests'].at[idx, 'Status'] = action
                            data['special_requests'].at[idx, 'Remarks'] = f"{remarks} - {data['special_requests'].at[idx, 'Remarks']}"
                        
                            # If approved, add to special contributions
                            if action == "Approve":
                                new_contribution = {
                                    'Event': data['special_requests'].at[idx, 'Event'],
                                    'Date': data['special_requests'].at[idx, 'Date'],
                                    'Type': data['special_requests'].at[idx, 'Type'],
                                    'Contributors': data['special_requests'].at[idx, 'Requested By'],
                                    'Amount': data['special_requests'].at[idx, 'Amount'],
                                    'Remarks': data['special_requests'].at[idx, 'Remarks']
                                }
                                data['special'] = pd.concat([data['special'], pd.DataFrame([new_contribution])], ignore_index=True)
                    
                        save_data(data)
                        st.success(f"{len(selected_requests)} special contribution requests {action.lower()}ed successfully!")
                        st.rerun()
            else:
                st.info("No pending special contribution requests")
    
    # Display upcoming events in sidebar
    upcoming_events = data['special'].copy()
    if not upcoming_events.empty:
        upcoming_events['Date'] = pd.to_datetime(upcoming_events['Date'])
        upcoming_events = upcoming_events[upcoming_events['Date'] >= datetime.now()]
        
        if not upcoming_events.empty:
            st.sidebar.subheader("📅 Upcoming Events")
            for _, event in upcoming_events.iterrows():
                emoji = "🎉" if event['Type'] == 'Celebration' else "⚠️" if event['Type'] == 'Emergency' else "🤝"
                st.sidebar.write(f"{emoji} {event['Event']} - {event['Date'].strftime('%b %d')}")
    
    # Display records
    st.subheader("📋 Special Contribution Records")
    
    # Filter
    type_filter = st.selectbox("Filter by Type", ["All"] + SPECIAL_TYPES)
    
    # Apply filter
    filtered_special = data['special'].copy()
    if type_filter != "All":
        filtered_special = filtered_special[filtered_special['Type'] == type_filter]
    
    # Display editable table - Only for treasurer
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
        
        if st.button("💾 Save Special Contribution Changes"):
            # Convert date strings back to datetime format before saving
            edited_special['Date'] = pd.to_datetime(edited_special['Date'])
            data['special'].loc[edited_special.index] = edited_special
            save_data(data)
            st.success("Special contribution changes saved successfully!")
    else:
        st.dataframe(
            filtered_special,
            use_container_width=True,
            hide_index=True
        )
    
    # Visualizations
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

def reports(data):
    st.header("📑 Financial Reports")
    
    def safe_sum(series):
        return sum(safe_convert_to_float(x) for x in series)
    
    # Summary statistics
    st.subheader("📊 Summary Statistics")
    cols = st.columns(3)
    
    with cols[0]:
        total_contributions = safe_sum(data['contributions']['YTD'])
        st.metric(
            "Total Regular Contributions", 
            f"KES {total_contributions:,.2f}",
            help="Sum of all regular monthly contributions YTD"
        )
    
    with cols[1]:
        total_expenses = safe_sum(data['expenses']['Amount (KES)']) if 'Amount (KES)' in data['expenses'].columns else 0
        st.metric(
            "Total Expenses", 
            f"KES {total_expenses:,.2f}",
            help="Sum of all recorded expenses"
        )
    
    with cols[2]:
        total_special = safe_sum(data['special']['Amount']) if 'Amount' in data['special'].columns else 0
        st.metric(
            "Total Special Contributions", 
            f"KES {total_special:,.2f}",
            help="Sum of all special contributions"
        )
    
    # Year selection for reports
    current_year = datetime.now().year
    report_year = st.selectbox("Select Year for Report", 
                             [current_year, current_year - 1],
                             index=0)
    
    # Detailed reports
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
    ], index=0)  # Default to "Lane-wise Contributions"
    
    if report_type == "Detailed Monthly Contributions":
        st.subheader("Detailed Monthly Contributions")
        st.dataframe(
            data['contributions'],
            use_container_width=True,
            hide_index=True
        )
        
    elif report_type == "Detailed Expense Records":
        st.subheader("Detailed Expense Records")
        st.dataframe(
            data['expenses'],
            use_container_width=True,
            hide_index=True
        )
        
    elif report_type == "Lane-wise Contributions":
        lane_report = data['contributions'].groupby('Lane').agg({
            'YTD': lambda x: safe_sum(x),
            'Current Debt': lambda x: safe_sum(x),
            'House No': 'count'
        }).rename(columns={'House No': 'Households'})
        
        st.dataframe(
            lane_report.style.format({
                'YTD': "KES {:,.2f}",
                'Current Debt': "KES {:,.2f}"
            }),
            use_container_width=True
        )
        
        # Combined Monthly Trend
        monthly_contrib = data['contributions'][MONTHS].apply(pd.to_numeric, errors='coerce').sum()
        monthly_expenses = data['expenses'].copy()
        monthly_expenses['Month'] = pd.to_datetime(monthly_expenses['Date']).dt.strftime('%b').str.upper()
        monthly_exp_totals = monthly_expenses.groupby('Month')['Amount (KES)'].sum().reindex(MONTHS, fill_value=0)
        
        # Create combined dataframe
        combined_df = pd.DataFrame({
            'Month': MONTHS,
            'Contributions': monthly_contrib,
            'Expenses': monthly_exp_totals
        }).melt(id_vars='Month', var_name='Type', value_name='Amount')
        
        fig = px.line(
            combined_df,
            x='Month',
            y='Amount',
            color='Type',
            title="Monthly Contribution vs Expense Trend",
            labels={'Amount': 'Amount (KES)', 'Month': 'Month'},
            markers=True
        )
        st.plotly_chart(fig, use_container_width=True)
    
    elif report_type == "Expense Category Breakdown":
        expense_report = data['expenses'].groupby('Category')['Amount (KES)'].apply(safe_sum)
        st.dataframe(
            expense_report.to_frame('Total Amount').style.format("KES {:,.2f}"),
            use_container_width=True
        )
        
        fig = px.pie(
            expense_report, 
            names=expense_report.index, 
            title="Expense Distribution by Category",
            color=expense_report.index,
            color_discrete_sequence=px.colors.qualitative.Pastel,
            values='Amount (KES)'
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Combined Monthly Trend
        monthly_contrib = data['contributions'][MONTHS].apply(pd.to_numeric, errors='coerce').sum()
        monthly_expenses = data['expenses'].copy()
        monthly_expenses['Month'] = pd.to_datetime(monthly_expenses['Date']).dt.strftime('%b').str.upper()
        monthly_exp_totals = monthly_expenses.groupby('Month')['Amount (KES)'].sum().reindex(MONTHS, fill_value=0)
        
        # Create combined dataframe
        combined_df = pd.DataFrame({
            'Month': MONTHS,
            'Contributions': monthly_contrib,
            'Expenses': monthly_exp_totals
        }).melt(id_vars='Month', var_name='Type', value_name='Amount')
        
        fig = px.line(
            combined_df,
            x='Month',
            y='Amount',
            color='Type',
            title="Monthly Contribution vs Expense Trend",
            labels={'Amount': 'Amount (KES)', 'Month': 'Month'},
            markers=True
        )
        st.plotly_chart(fig, use_container_width=True)
    
    elif report_type == "Payment Status Distribution":
        status_report = data['contributions']['Status'].value_counts()
        st.dataframe(
            status_report.to_frame('Households'),
            use_container_width=True
        )
        
        fig = px.pie(
            status_report, 
            names=status_report.index, 
            title="Payment Status Distribution",
            color=status_report.index,
            color_discrete_sequence=['#2ecc71', '#f39c12', '#e74c3c'],  # Green, Orange, Red
            values='Households'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    elif report_type == "Rate Category Analysis":
        if 'Rate Category' in data['contributions'].columns:
            rate_report = data['contributions'].groupby('Rate Category').agg({
                'YTD': lambda x: safe_sum(x),
                'House No': 'count'
            }).rename(columns={'House No': 'Households'})
            
            # Merge with rate amounts
            rate_report = rate_report.merge(
                data['rates'].set_index('Rate Category'),
                left_index=True,
                right_index=True
            )
            
            st.dataframe(
                rate_report.style.format({
                    'YTD': "KES {:,.2f}",
                    'Amount': "KES {:,.2f}"
                }),
                use_container_width=True
            )
            
            fig = px.bar(
                rate_report,
                x=rate_report.index,
                y='Households',
                title="Households by Rate Category",
                color=rate_report.index,
                color_discrete_sequence=px.colors.qualitative.Pastel
            )
            st.plotly_chart(fig, use_container_width=True)
    
    elif report_type == "Special Contributions Analysis":
        if not data['special'].empty:
            special_report = data['special'].groupby('Type')['Amount'].apply(safe_sum)
            st.dataframe(
                special_report.to_frame('Total Amount').style.format("KES {:,.2f}"),
                use_container_width=True
            )
            
            fig = px.pie(
                special_report, 
                names=special_report.index, 
                title="Special Contributions by Type",
                color=special_report.index,
                color_discrete_sequence=px.colors.qualitative.Pastel,
                values='Amount'
            )
            st.plotly_chart(fig, use_container_width=True)
            
    elif report_type == "Year-on-Year Trends":
        st.subheader("Year-on-Year Financial Trends")
        
        # Placeholder for year-on-year comparison
        # In a real implementation, you would need historical data
        st.info("Year-on-year comparison requires historical data from previous years")
        
        # Example of how this could work with actual historical data
        if st.checkbox("Show Example with Sample Data"):
            years = [current_year - 1, current_year]
            sample_data = pd.DataFrame({
                'Year': years,
                'Total Contributions': [safe_sum(data['contributions']['YTD']) * 0.8, 
                                       safe_sum(data['contributions']['YTD'])],
                'Total Expenses': [safe_sum(data['expenses']['Amount (KES)']) * 0.75 if 'Amount (KES)' in data['expenses'].columns else 0,
                                  safe_sum(data['expenses']['Amount (KES)']) if 'Amount (KES)' in data['expenses'].columns else 0],
                'Special Contributions': [safe_sum(data['special']['Amount']) * 0.7 if 'Amount' in data['special'].columns else 0,
                                         safe_sum(data['special']['Amount']) if 'Amount' in data['special'].columns else 0]
            })
            
            fig = px.bar(
                sample_data.melt(id_vars='Year'),
                x='Year',
                y='value',
                color='variable',
                barmode='group',
                title="Year-on-Year Financial Comparison",
                labels={'value': 'Amount (KES)', 'variable': 'Category'}
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # Export reports - available to all members
    st.subheader("📤 Export Reports")
    
    # Generate report based on current view
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
            }).rename(columns={'House No': 'Households'})
            report_df = report_df.merge(
                data['rates'].set_index('Rate Category'),
                left_index=True,
                right_index=True
            )
    elif report_type == "Special Contributions Analysis":
        if not data['special'].empty:
            report_df = data['special'].groupby('Type')['Amount'].apply(safe_sum).to_frame('Total Amount')
    
    if report_df is not None:
        # Create Excel file in memory
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            report_df.to_excel(writer, sheet_name=report_type[:30])
        
        # Create download button
        st.download_button(
            label="⬇️ Download Current Report",
            data=output.getvalue(),
            file_name=f"zawadi_{report_type.lower().replace(' ', '_')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    
    # Export full report - Only for treasurer
    if st.session_state.get('treasurer_authenticated', False):
        st.subheader("📤 Full Data Export")
        export_options = st.multiselect(
            "Select data to export",
            options=[
                "Contributions Data",
                "Expenses Data",
                "Special Contributions",
                "Rate Categories",
                "Expense Requests",
                "Contribution Requests",
                "Special Requests"
            ],
            default=[
                "Contributions Data",
                "Expenses Data",
                "Special Contributions"
            ]
        )
        
        if st.button("🖨️ Generate Custom Report"):
            with pd.ExcelWriter("zawadi_custom_report.xlsx") as writer:
                # Add selected sheets
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
                
                # Add summary sheet
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

# Unit tests
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
        mock_read_csv.side_effect = FileNotFoundError
        data = load_data()
        self.assertTrue('contributions' in data)
        self.assertTrue('expenses' in data)
        self.assertTrue('special' in data)
        self.assertTrue('rates' in data)
    
    @patch('smtplib.SMTP')
    def test_send_reminder_email(self, mock_smtp):
        mock_server = MagicMock()
        mock_smtp.return_value = mock_server
        result = send_reminder_email("test@example.com", "Test Family", 5000)
        self.assertTrue(result)
        mock_smtp.assert_called_once()

def main():
    data = load_data()
    
    # Check for monthly reminders
    if datetime.now().day == 1:
        send_monthly_reminders(data)
    
    st.sidebar.title("🏠 Zawadi Court Welfare")
    
    # Check treasurer authentication only when needed
    if 'treasurer_authenticated' not in st.session_state:
        st.session_state.treasurer_authenticated = False
    
    # Password input in sidebar
    check_treasurer_password()
    
    # Navigation - responsive layout
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
    
    # Residency management in sidebar (collapsible) - Only for treasurer
    if st.session_state.get('treasurer_authenticated', False):
        residency_management(data)
    
    # Show the selected page
    if page == "Contributions Dashboard":
        contributions_dashboard(data)
    elif page == "Expense Tracker":
        expense_tracker(data)
    elif page == "Special Contributions":
        special_contributions(data)
    elif page == "Reports":
        reports(data)
    
    # Data management in sidebar - Only for treasurer
    if st.session_state.get('treasurer_authenticated', False):
        st.sidebar.subheader("💾 Data Management")
        if st.sidebar.button("🔁 Backup All Data"):
            save_data(data)
            st.sidebar.success("Data backup completed!")
        
        uploaded_file = st.sidebar.file_uploader(
            "Restore Data", 
            type=['csv', 'xlsx'], 
            accept_multiple_files=True,
            help="Upload CSV files for contributions, expenses, or special data"
        )
        if uploaded_file and st.sidebar.button("🔄 Restore Data"):
            try:
                for file in uploaded_file:
                    if 'contributions' in file.name.lower():
                        data['contributions'] = pd.read_csv(file)
                    elif 'expenses' in file.name.lower():
                        data['expenses'] = pd.read_csv(file)
                    elif 'special' in file.name.lower():
                        data['special'] = pd.read_csv(file)
                    elif 'rates' in file.name.lower():
                        data['rates'] = pd.read_csv(file)
                    elif 'expense_requests' in file.name.lower():
                        data['expense_requests'] = pd.read_csv(file)
                    elif 'contribution_requests' in file.name.lower():
                        data['contribution_requests'] = pd.read_csv(file)
                    elif 'special_requests' in file.name.lower():
                        data['special_requests'] = pd.read_csv(file)
                
                save_data(data)
                st.sidebar.success("Data restored successfully!")
                st.rerun()
            except Exception as e:
                st.sidebar.error(f"Error restoring data: {e}")
    
    # Run tests if in development mode
    if os.getenv('DEV_MODE'):
        test_results = unittest.TextTestRunner().run(unittest.TestLoader().loadTestsFromTestCase(TestZawadiFunctions))
        if not test_results.wasSuccessful():
            st.error("Some tests failed. Check the logs for details.")

if __name__ == "__main__":
    main()