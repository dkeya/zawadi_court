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
