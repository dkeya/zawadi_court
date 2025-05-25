import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from datetime import datetime
import os
import hashlib

# Page configuration
st.set_page_config(
    page_title="Zawadi Court Welfare System",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Constants
LANES = ['ROYAL', 'SHUJAA', 'WEMA', 'KINGS']
MONTHS = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
EXPENSE_CATEGORIES = ['Personnel', 'Utilities', 'Maintenance', 'Miscellaneous']
SOCIAL_TYPES = ['Celebration', 'Emergency', 'Welfare']
DEFAULT_RATES = {'Resident': 2000, 'Non-Resident': 1000, 'Special Rate': 500}
TREASURER_PASSWORD = "zawadi01*"

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
        'social': 'data/social.csv',
        'rates': 'data/rates.csv',
        'expense_requests': 'data/expense_requests.csv'
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
                # Convert date column to string if it exists
                if 'Date' in df.columns:
                    df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
                if 'Amount (KES)' in df.columns:
                    df['Amount (KES)'] = df['Amount (KES)'].apply(safe_convert_to_float)
                data[name] = df
            elif name == 'social':
                df = pd.read_csv(file)
                # Convert date column to string if it exists
                if 'Date' in df.columns:
                    df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
                if 'Amount' in df.columns:
                    df['Amount'] = df['Amount'].apply(safe_convert_to_float)
                data[name] = df
            elif name == 'expense_requests':
                df = pd.read_csv(file)
                if 'Date' in df.columns:
                    df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
                if 'Amount (KES)' in df.columns:
                    df['Amount (KES)'] = df['Amount (KES)'].apply(safe_convert_to_float)
                data[name] = df
                    
        except FileNotFoundError:
            if name == 'contributions':
                columns = ['House No', 'Family Name', 'Lane', 'Rate Category', 
                          'Cumulative Debt (2024 & Prior)'] + MONTHS + ['YTD', 'Current Debt', 'Remarks']
                data[name] = pd.DataFrame(columns=columns)
            elif name == 'expenses':
                columns = ['Date', 'Description', 'Category', 'Vendor', 'Amount (KES)', 'Mode', 'Remarks', 'Receipt']
                data[name] = pd.DataFrame(columns=columns)
            elif name == 'social':
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
    
    return data

def save_data(data):
    """Save data to CSV files"""
    for name, df in data.items():
        df.to_csv(f'data/{name}.csv', index=False)

def check_treasurer_password():
    """Check if user has entered the treasurer password"""
    if 'treasurer_authenticated' not in st.session_state:
        st.session_state.treasurer_authenticated = False

    if not st.session_state.treasurer_authenticated:
        password = st.sidebar.text_input("Enter Treasurer Password:", type="password", key="treasurer_pw")
        if password:
            if password == TREASURER_PASSWORD:
                st.session_state.treasurer_authenticated = True
                st.rerun()
            else:
                st.sidebar.error("Incorrect password")
        return False
    return True

def get_current_month():
    """Get current month as 3-letter abbreviation"""
    return datetime.now().strftime('%b').upper()

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
            household_rates = data['contributions'][['House No', 'Family Name', 'Rate Category']].copy()
            
            edited_household_rates = st.data_editor(
                household_rates,
                column_config={
                    "House No": st.column_config.TextColumn("House No", disabled=True),
                    "Family Name": st.column_config.TextColumn("Family Name", disabled=True),
                    "Rate Category": st.column_config.SelectboxColumn(
                        "Rate Category",
                        options=rate_options,
                        required=True
                    )
                },
                use_container_width=True,
                hide_index=True,
                key="household_rates_editor"
            )
            
            if st.button("Apply Rate Categories"):
                data['contributions']['Rate Category'] = data['contributions']['House No'].map(
                    edited_household_rates.set_index('House No')['Rate Category']
                )
                save_data(data)
                st.success("Rate categories applied to households!")
        else:
            st.warning("No household data available to assign rate categories")

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

def get_payment_status(row, current_month):
    """Determine payment status for a household"""
    month_index = MONTHS.index(current_month) if current_month in MONTHS else 11
    months_owed = 0
    
    for i in range(month_index + 1):
        month_val = safe_convert_to_float(row[MONTHS[i]])
        if month_val == 0:
            months_owed += 1
    
    if months_owed == 0:
        return "🟢 Up-to-date"
    elif months_owed <= 2:
        return "🟠 1-2 months behind"
    else:
        return "🔴 >2 months behind"

def contributions_dashboard(data):
    st.header("📊 Monthly Contributions Dashboard")
    
    current_month = get_current_month()
    
    # Calculate metrics
    data['contributions']['YTD'] = data['contributions'].apply(
        lambda row: calculate_ytd(row, current_month), axis=1
    )
    data['contributions']['Current Debt'] = data['contributions'].apply(
        lambda row: calculate_current_debt(row, current_month, data['rates']), axis=1
    )
    data['contributions']['Status'] = data['contributions'].apply(
        lambda row: get_payment_status(row, current_month), axis=1
    )
    
    # Filters - Added Family Name filter first as requested
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
    
    editable_cols = ['House No', 'Family Name', 'Lane', 'Rate Category', 
                    'Cumulative Debt (2024 & Prior)'] + months_to_show + ['Remarks']
    
    # Only allow editing if treasurer is authenticated
    if st.session_state.get('treasurer_authenticated', False):
        edited_df = st.data_editor(
            filtered_df[editable_cols + ['YTD', 'Current Debt', 'Status']],
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
                **{col: st.column_config.NumberColumn(format="%d") for col in months_to_show + ['YTD', 'Current Debt', 'Cumulative Debt (2024 & Prior)']}
            },
            use_container_width=True,
            num_rows="dynamic",
            key="contributions_editor"
        )
        
        if st.button("💾 Save Changes"):
            for col in editable_cols:
                data['contributions'].loc[edited_df.index, col] = edited_df[col]
            save_data(data)
            st.success("Changes saved successfully!")
    else:
        st.dataframe(
            filtered_df[editable_cols + ['YTD', 'Current Debt', 'Status']],
            use_container_width=True,
            hide_index=True
        )
        st.info("🔒 Enter treasurer password in sidebar to edit data")
    
    # Visualizations
    st.subheader("📈 Contributions Analysis")
    col1, col2 = st.columns(2)
    
    with col1:
        monthly_totals = filtered_df[months_to_show].apply(pd.to_numeric, errors='coerce').sum()
        fig = px.bar(
            monthly_totals, 
            title=f"Monthly Collections ({datetime.now().year})",
            labels={'value': 'Amount (KES)', 'index': 'Month'},
            color=monthly_totals.index,
            color_discrete_sequence=px.colors.qualitative.Pastel
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        if 'Rate Category' in filtered_df.columns:
            rate_totals = filtered_df.groupby('Rate Category')['YTD'].sum()
            fig = px.pie(
                rate_totals, 
                names=rate_totals.index, 
                title="Contributions by Rate Category",
                color=rate_totals.index,
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
    
    # Expense Requisition Section - New addition
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
            req_remarks = st.text_area("Remarks")
            
            if st.form_submit_button("Submit Requisition"):
                new_request = {
                    'Date': req_date.strftime('%Y-%m-%d'),
                    'Category': req_category,
                    'Description': req_description,
                    'Requested By': req_name,
                    'Amount (KES)': req_amount,
                    'Status': 'Pending Approval',
                    'Remarks': req_remarks
                }
                
                data['expense_requests'] = pd.concat([data['expense_requests'], pd.DataFrame([new_request])], ignore_index=True)
                save_data(data)
                st.success("Expense requisition submitted for approval!")
                st.rerun()
    
    # Approve Expense Requisitions - New addition
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
                        selected = st.checkbox(f"Select #{idx}", key=f"req_select_{idx}")
                    with cols[1]:
                        st.write(f"**{request['Description']}** - KES {request['Amount (KES)']:,.2f}")
                        st.caption(f"Requested by: {request['Requested By']} | Category: {request['Category']}")
                        st.caption(f"Remarks: {request['Remarks']}")
                    
                    if selected:
                        selected_requests.append(idx)
                
                if selected_requests:
                    action = st.selectbox("Action for selected requests", ["Approve", "Reject"])
                    remarks = st.text_area("Approval Remarks", "Approved by treasurer")
                    
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
                                    'Amount (KES)': data['expense_requests'].at[idx, 'Amount (KES)'],
                                    'Mode': "To be determined",
                                    'Remarks': f"Approved from requisition: {data['expense_requests'].at[idx, 'Remarks']}",
                                    'Receipt': None
                                }
                                data['expenses'] = pd.concat([data['expenses'], pd.DataFrame([new_expense])], ignore_index=True)
                        
                        save_data(data)
                        st.success(f"{len(selected_requests)} requests {action.lower()}ed successfully!")
                        st.rerun()
            else:
                st.info("No pending expense requisitions")
    else:
        st.info("🔒 Treasurer access required to approve expense requisitions")
    
    # Cash balance section
    st.subheader("💰 Cash Flow Management")
    cash_cols = st.columns(3)
    with cash_cols[0]:
        cash_balance_cd = st.number_input("Cash Balance c/d (KES)", min_value=0, value=0, step=1000)
    with cash_cols[1]:
        cash_withdrawal = st.number_input("Cash Withdrawal (KES)", min_value=0, value=0, step=1000)
    
    # Calculate and display cash summary
    total_expenses = data['expenses']['Amount (KES)'].sum() if 'Amount (KES)' in data['expenses'].columns else 0
    cash_balance_bf = cash_balance_cd + cash_withdrawal - total_expenses
    
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
                expense_remarks = st.text_area("Remarks")
                expense_receipt = st.file_uploader("Upload Receipt (optional)", type=['png', 'jpg', 'pdf'])
                
                if st.form_submit_button("Add Expense"):
                    new_expense = {
                        'Date': expense_date.strftime('%Y-%m-%d'),
                        'Category': expense_category,
                        'Description': expense_description,
                        'Vendor': expense_vendor,
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
    else:
        st.info("🔒 Treasurer access required to add expenses directly. Please submit a requisition above.")
    
    # Expense records
    st.subheader("📝 Expense Records")
    
    # Filters
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
    
    # Visualizations
    st.subheader("📊 Expense Analysis")
    viz_cols = st.columns(2)
    
    with viz_cols[0]:
        if not filtered_expenses.empty:
            category_totals = filtered_expenses.groupby('Category')['Amount (KES)'].sum()
            fig = px.pie(
                category_totals, 
                names=category_totals.index, 
                title="Expense Breakdown by Category",
                color=category_totals.index,
                color_discrete_sequence=px.colors.qualitative.Pastel
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

def social_contributions(data):
    st.header("🎉 Social Contributions")
    
    # Add new contribution
    with st.expander("➕ Add New Social Contribution", expanded=False):
        with st.form("social_form"):
            cols = st.columns(2)
            with cols[0]:
                event_name = st.text_input("Event Name")
                event_date = st.date_input("Event Date", datetime.now())
            with cols[1]:
                event_type = st.selectbox("Type", SOCIAL_TYPES)
                event_amount = st.number_input("Amount (KES)", min_value=0)
            
            contributors = st.multiselect("Contributors", data['contributions']['Family Name'].unique())
            event_remarks = st.text_area("Remarks")
            
            if st.form_submit_button("Add Contribution"):
                new_contribution = {
                    'Event': event_name,
                    'Date': event_date.strftime('%Y-%m-%d'),
                    'Type': event_type,
                    'Contributors': ", ".join(contributors),
                    'Amount': event_amount,
                    'Remarks': event_remarks
                }
                
                data['social'] = pd.concat([data['social'], pd.DataFrame([new_contribution])], ignore_index=True)
                save_data(data)
                st.success("Social contribution added successfully!")
                st.rerun()
    
    # Display upcoming events in sidebar
    upcoming_events = data['social'].copy()
    if not upcoming_events.empty:
        upcoming_events['Date'] = pd.to_datetime(upcoming_events['Date'])
        upcoming_events = upcoming_events[upcoming_events['Date'] >= datetime.now()]
        
        if not upcoming_events.empty:
            st.sidebar.subheader("📅 Upcoming Events")
            for _, event in upcoming_events.iterrows():
                emoji = "🎉" if event['Type'] == 'Celebration' else "⚠️" if event['Type'] == 'Emergency' else "🤝"
                st.sidebar.write(f"{emoji} {event['Event']} - {event['Date'].strftime('%b %d')}")
    
    # Display records
    st.subheader("📋 Social Contribution Records")
    
    # Filter
    type_filter = st.selectbox("Filter by Type", ["All"] + SOCIAL_TYPES)
    
    # Apply filter
    filtered_social = data['social'].copy()
    if type_filter != "All":
        filtered_social = filtered_social[filtered_social['Type'] == type_filter]
    
    # Display editable table - Only for treasurer
    if st.session_state.get('treasurer_authenticated', False):
        edited_social = st.data_editor(
            filtered_social,
            column_config={
                "Amount": st.column_config.NumberColumn("Amount (KES)", format="%d"),
                "Type": st.column_config.SelectboxColumn("Type", options=SOCIAL_TYPES),
                "Date": st.column_config.TextColumn("Date (YYYY-MM-DD)")
            },
            use_container_width=True,
            num_rows="dynamic",
            key="social_editor",
            hide_index=True
        )
        
        if st.button("💾 Save Social Contribution Changes"):
            # Convert date strings back to datetime format before saving
            edited_social['Date'] = pd.to_datetime(edited_social['Date'])
            data['social'].loc[edited_social.index] = edited_social
            save_data(data)
            st.success("Social contribution changes saved successfully!")
    else:
        st.dataframe(
            filtered_social,
            use_container_width=True,
            hide_index=True
        )
    
    # Visualizations
    st.subheader("📊 Social Contributions Analysis")
    
    if not filtered_social.empty:
        viz_cols = st.columns(2)
        
        with viz_cols[0]:
            type_totals = filtered_social.groupby('Type')['Amount'].sum()
            fig = px.pie(
                type_totals, 
                names=type_totals.index, 
                title="Contributions by Type",
                color=type_totals.index,
                color_discrete_sequence=px.colors.qualitative.Pastel
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with viz_cols[1]:
            monthly_social = filtered_social.copy()
            monthly_social['Month'] = pd.to_datetime(monthly_social['Date']).dt.strftime('%b').str.upper()
            monthly_totals = monthly_social.groupby('Month')['Amount'].sum().reindex(MONTHS, fill_value=0)
            fig = px.bar(
                monthly_totals, 
                title="Monthly Social Contributions",
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
        total_social = safe_sum(data['social']['Amount']) if 'Amount' in data['social'].columns else 0
        st.metric(
            "Total Social Contributions", 
            f"KES {total_social:,.2f}",
            help="Sum of all social contributions"
        )
    
    # Detailed reports
    st.subheader("📋 Detailed Reports")
    report_type = st.selectbox("Select Report Type", [
        "Lane-wise Contributions",
        "Expense Category Breakdown", 
        "Payment Status Distribution",
        "Rate Category Analysis",
        "Social Contributions Analysis"
    ])
    
    if report_type == "Lane-wise Contributions":
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
        
        fig = px.bar(
            lane_report, 
            y='YTD',
            title="Total Contributions by Lane",
            labels={'YTD': 'Amount (KES)', 'Lane': 'Lane'},
            color=lane_report.index,
            color_discrete_sequence=px.colors.qualitative.Pastel
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Combined Monthly Trend - New addition
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
            color_discrete_sequence=px.colors.qualitative.Pastel
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Combined Monthly Trend - New addition
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
            color_discrete_sequence=['#2ecc71', '#f39c12', '#e74c3c']  # Green, Orange, Red
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
    
    elif report_type == "Social Contributions Analysis":
        if not data['social'].empty:
            social_report = data['social'].groupby('Type')['Amount'].apply(safe_sum)
            st.dataframe(
                social_report.to_frame('Total Amount').style.format("KES {:,.2f}"),
                use_container_width=True
            )
            
            fig = px.pie(
                social_report, 
                names=social_report.index, 
                title="Social Contributions by Type",
                color=social_report.index,
                color_discrete_sequence=px.colors.qualitative.Pastel
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # Export full report - Only for treasurer
    if st.session_state.get('treasurer_authenticated', False):
        st.subheader("📤 Export Full Report")
        if st.button("🖨️ Generate Full Report"):
            with pd.ExcelWriter("zawadi_full_report.xlsx") as writer:
                # Summary sheet
                pd.DataFrame({
                    'Metric': ['Total Regular Contributions', 'Total Expenses', 'Total Social Contributions'],
                    'Amount (KES)': [total_contributions, total_expenses, total_social]
                }).to_excel(writer, sheet_name="Summary", index=False)
                
                # Detail sheets
                data['contributions'].to_excel(writer, sheet_name="Contributions")
                data['expenses'].to_excel(writer, sheet_name="Expenses")
                data['social'].to_excel(writer, sheet_name="Social")
                if 'rates' in data:
                    data['rates'].to_excel(writer, sheet_name="Rate Categories")
                if 'expense_requests' in data:
                    data['expense_requests'].to_excel(writer, sheet_name="Expense Requests")
            
            with open("zawadi_full_report.xlsx", "rb") as f:
                st.download_button(
                    "⬇️ Download Full Report",
                    f,
                    "zawadi_full_report.xlsx",
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
    else:
        st.info("🔒 Treasurer access required to export full reports")

def main():
    data = load_data()
    
    st.sidebar.title("🏠 Zawadi Court Welfare")
    
    # Check treasurer authentication only when needed
    if 'treasurer_authenticated' not in st.session_state:
        st.session_state.treasurer_authenticated = False
    
    # Password input in sidebar
    check_treasurer_password()
    
    # Navigation
    page = st.sidebar.radio(
        "Navigation",
        ["Contributions Dashboard", "Expense Tracker", "Social Contributions", "Reports"],
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
    elif page == "Social Contributions":
        social_contributions(data)
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
            help="Upload CSV files for contributions, expenses, or social data"
        )
        if uploaded_file and st.sidebar.button("🔄 Restore Data"):
            try:
                for file in uploaded_file:
                    if 'contributions' in file.name.lower():
                        data['contributions'] = pd.read_csv(file)
                    elif 'expenses' in file.name.lower():
                        data['expenses'] = pd.read_csv(file)
                    elif 'social' in file.name.lower():
                        data['social'] = pd.read_csv(file)
                    elif 'rates' in file.name.lower():
                        data['rates'] = pd.read_csv(file)
                    elif 'expense_requests' in file.name.lower():
                        data['expense_requests'] = pd.read_csv(file)
                
                save_data(data)
                st.sidebar.success("Data restored successfully!")
                st.rerun()
            except Exception as e:
                st.sidebar.error(f"Error restoring data: {e}")

if __name__ == "__main__":
    main()